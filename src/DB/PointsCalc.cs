using CounterStrikeSharp.API;
using CounterStrikeSharp.API.Core;
using CounterStrikeSharp.API.Core.Attributes.Registration;
using CounterStrikeSharp.API.Modules.Admin;
using CounterStrikeSharp.API.Modules.Commands;
using CounterStrikeSharp.API.Modules.Utils;
using MySqlConnector;
using System.Data;
using System.Data.Common;

namespace SharpTimer
{
    partial class SharpTimer
    {
        [ConsoleCommand("css_resetpoints", "Resets points to zero. Used before doing a points recalculation.")]
        [RequiresPermissions("@css/root")]
        [CommandHelper(whoCanExecute: CommandUsage.CLIENT_AND_SERVER)]
        public void ResetPlayerPointsCommand(CCSPlayerController? player, CommandInfo command)
        {
            _ = Task.Run(ResetPlayerPoints);
            player?.PrintToChat($"{Localizer["prefix"]} Points have been reset!");
        }

        [ConsoleCommand("css_importpoints", " ")]
        [RequiresPermissions("@css/root")]
        [CommandHelper(whoCanExecute: CommandUsage.CLIENT_AND_SERVER)]
        public void ImportPlayerPointsCommand(CCSPlayerController? player, CommandInfo command)
        {
            _ = Task.Run(ImportPlayerPoints);
        }

        public async Task ResetPlayerPoints()
        {
            using (var connection = await OpenConnectionAsync())
            {
                try
                {
                    await CreatePlayerStatsTableAsync(connection);

                    string updateQuery = $@"UPDATE `{PlayerStatsTable}` SET `GlobalPoints` = 0;";
                    DbCommand updateCommand = new MySqlCommand(updateQuery, (MySqlConnection)connection);

                    using (updateCommand)
                    {
                        await updateCommand!.ExecuteNonQueryAsync();
                    }
                }
                catch (Exception ex)
                {
                    SharpTimerError($"Error in ResetPlayerPoints: {ex.Message}");
                }
            }
        }

        public async Task ImportPlayerPoints()
        {
            try
            {
                Server.NextFrame(() => PrintToChatAll($"{Localizer["prefix"]} {ChatColors.LightRed}Points recalculation initialized."));

                var styles = Enumerable.Range(0, 13).ToArray();

                foreach (var styleValue in styles)
                {
                    var all = await GetAllSortedRecordsFromDatabase(limit: 0, bonusX: 0, style: styleValue);

                    foreach (var grp in all.GroupBy(r => r.SteamID!))
                    {
                        int total = 0;
                        string steamId = grp.Key!;
                        string playerName = grp.First().PlayerName!;

                        foreach (var rec in grp.OrderBy(r => r.TimerTicks))
                        {
                            // How many times this player finished THIS exact row (map+bonus suffix, style)
                            int times = await GetTimesFinishedAsync(steamId, rec.MapName!, styleValue);
                            if (times <= 0) continue;

                            // Respect the server cap
                            int effective = (globalPointsMaxCompletions > 0)
                                ? Math.Min(times, globalPointsMaxCompletions)
                                : times;

                            // Points awarded for a single completion of this record
                            int perCompletion = await CalculatePlayerPoints(
                                steamId,
                                playerName,
                                rec.TimerTicks,
                                oldTicks: 0,
                                beatPB: false,
                                bonusX: rec.BonusX,
                                style: styleValue,
                                completions: 0,
                                mapname: rec.MapName!,
                                forGlobal: false
                            );

                            total += perCompletion * effective;
                        }

                        await UpsertGlobalPointsAsync(steamId, playerName, total);
                    }
                }

                Server.NextFrame(() => PrintToChatAll($"{Localizer["prefix"]} {ChatColors.Lime}Points recalculation completed!"));
            }
            catch (Exception ex)
            {
                SharpTimerError($"Error ImportPlayerPoints to the database: {ex.Message}");
            }
        }

        public async Task<int> CalculatePlayerPoints(string steamId, string playerName, int timerTicks, int oldTicks, bool beatPB = false, int bonusX = 0, int style = 0, int completions = 0, string mapname = "", bool forGlobal = false)
        {
            SharpTimerDebug($"Trying to calculate player points for {playerName}");
            try
            {
                if (mapname == "")
                    mapname = currentMapName!;

                // First calculate baseline map completion points based on tier
                double newPoints = CalculateCompletion(forGlobal);

                if (completions != 0 && globalPointsMaxCompletions > 0 &&
                    await PlayerCompletions(steamId, bonusX, style, mapname) > globalPointsMaxCompletions && !beatPB)
                    return 0;

                // Bonus AND Style → zero for all runs
                if (bonusX != 0 && style != 0)
                    return 0;

                // Bonus only → baseline only
                if (bonusX != 0)
                    return (int)Math.Round(newPoints);

                // Style only → baseline × multiplier (for non-global)
                if (style != 0 && !forGlobal)
                {
                    if (enableStylePoints)
                        newPoints *= GetStyleMultiplier(style);
                    return (int)Math.Round(newPoints);
                }

                // Global‐points guard (zero only on bonus+style, otherwise baseline)
                if (forGlobal)
                {
                    if (bonusX != 0 && style != 0)
                        return 0;
                    return (int)Math.Round(newPoints);
                }

                // Standard run (bonusX==0 && style==0) → apply Top-10 / Group bonuses
                var sortedRecords = await GetSortedRecordsFromDatabase(0, bonusX, mapname, style)
                                    ?? new Dictionary<int, PlayerRecord>();
                double maxPoints = await CalculateTier(sortedRecords.Count, mapname);

                int rank = 1;
                bool isTop10 = false;
                if (sortedRecords.Count == 0)
                {
                    newPoints += CalculateTop10(maxPoints, rank, forGlobal);
                    SharpTimerDebug($"First map entry, player {playerName} is rank #1");
                    isTop10 = true;
                }
                else
                {
                    foreach (var kvp in sortedRecords.Take(10))
                    {
                        if (kvp.Value.TimerTicks >= timerTicks)
                        {
                            newPoints += CalculateTop10(maxPoints, rank);
                            isTop10 = true;
                            SharpTimerDebug($"Player {playerName} is rank #{rank}");
                            break;
                        }
                        rank++;
                    }
                }

                // If not in top 10, calculate groups based on percentile
                if (!isTop10)
                {
                    newPoints += CalculateGroups(
                        maxPoints,
                        await GetPlayerMapPercentile(steamId, playerName, mapname, bonusX, style, timerTicks)
                    );
                }

                // Round & import-mode check
                newPoints = Math.Round(newPoints);
                if (completions == 0)
                    return (int)newPoints;

                // Zero out new points if player has exceeded max completions and has not set a pb
                if (globalPointsMaxCompletions > 0 && await PlayerCompletions(steamId, bonusX, style, mapname) > globalPointsMaxCompletions && !beatPB)
                {
                    newPoints = 0;
                }

                return (int)newPoints;
            }
            catch (Exception ex)
            {
                Server.NextFrame(() => SharpTimerError($"Error calculating player points for {playerName}: {ex}"));
            }
            return 0;
        }

        public async Task SavePlayerPoints(string steamId, string playerName, int playerSlot, int timerTicks, int oldTicks, bool beatPB = false, int bonusX = 0, int style = 0, int completions = 0, string mapname = "", bool import = false)
        {
            SharpTimerDebug($"Trying to set player points in database for {playerName}");
            try
            {
                if (mapname == "") mapname = currentMapName!;

                // If we're importing points, we need to fix mapname and bonusX
                if (bonusX == 0)
                    (mapname, bonusX) = FixMapAndBonus(mapname);

                int timeNowUnix = (int)DateTimeOffset.UtcNow.ToUnixTimeSeconds();
                int timesConnected = 0;
                int lastConnected = 0;
                bool isVip = false;
                string bigGif = "x";
                int playerPoints = 0;

                using (var connection = await OpenConnectionAsync())
                {
                    await CreatePlayerStatsTableAsync(connection);

                    string selectQuery = $@"
                        SELECT `PlayerName`, `TimesConnected`, `IsVip`, `BigGifID`, `GlobalPoints`
                        FROM `{PlayerStatsTable}`
                        WHERE `SteamID` = @SteamID;";

                    DbCommand selectCommand = new MySqlCommand(selectQuery, (MySqlConnection)connection);

                    using (selectCommand)
                    {
                        if (import)
                            selectCommand!.CommandTimeout = 120;

                        selectCommand!.AddParameterWithValue("@SteamID", steamId);

                        var row = await selectCommand!.ExecuteReaderAsync();

                        if (row.Read())
                        {
                            // Get player columns
                            timesConnected = row.GetInt32("TimesConnected");
                            isVip = row.GetBoolean("IsVip");
                            bigGif = row.GetString("BigGifID");
                            playerPoints = row.GetInt32("GlobalPoints");


                            int newPoints = await CalculatePlayerPoints(steamId, playerName, timerTicks, oldTicks, beatPB, bonusX, style, completions, mapname, false) + playerPoints;

                            await row.CloseAsync();

                            // Update or insert the record
                            string upsertQuery = $@"
                                INSERT INTO `{PlayerStatsTable}`
                                    (`PlayerName`, `SteamID`, `TimesConnected`, `LastConnected`, `HideTimerHud`, `HideKeys`,
                                    `SoundsEnabled`, `PlayerFov`, `HudType`, `IsVip`, `BigGifID`, `GlobalPoints`,
                                    `HideWeapon`, `HidePlayers`)
                                VALUES
                                    (@PlayerName, @SteamID, @TimesConnected, @LastConnected, @HideTimerHud, @HideKeys,
                                    @SoundsEnabled, @PlayerFov, @HudType, @IsVip, @BigGifID, @GlobalPoints,
                                    @HideWeapon, @HidePlayers)
                                ON DUPLICATE KEY UPDATE
                                    `PlayerName`     = VALUES(`PlayerName`),
                                    `TimesConnected` = VALUES(`TimesConnected`),
                                    `LastConnected`  = VALUES(`LastConnected`),
                                    `HideTimerHud`   = VALUES(`HideTimerHud`),
                                    `HideKeys`       = VALUES(`HideKeys`),
                                    `SoundsEnabled`  = VALUES(`SoundsEnabled`),
                                    `PlayerFov`      = VALUES(`PlayerFov`),
                                    `HudType`        = VALUES(`HudType`),
                                    `IsVip`          = VALUES(`IsVip`),
                                    `BigGifID`       = VALUES(`BigGifID`),
                                    `GlobalPoints`   = VALUES(`GlobalPoints`),
                                    `HideWeapon`     = VALUES(`HideWeapon`),
                                    `HidePlayers`    = VALUES(`HidePlayers`);";

                            DbCommand upsertCommand = new MySqlCommand(upsertQuery, (MySqlConnection)connection);

                            using (upsertCommand)
                            {
                                if (playerTimers.TryGetValue(playerSlot, out PlayerTimerInfo? value) || playerSlot == -1)
                                {
                                    upsertCommand!.AddParameterWithValue("@PlayerName", playerName);
                                    upsertCommand!.AddParameterWithValue("@SteamID", steamId);
                                    upsertCommand!.AddParameterWithValue("@TimesConnected", timesConnected);
                                    upsertCommand!.AddParameterWithValue("@LastConnected", lastConnected);
                                    upsertCommand!.AddParameterWithValue("@HideTimerHud", playerSlot != -1 && value!.HideTimerHud);
                                    upsertCommand!.AddParameterWithValue("@HideKeys", playerSlot != -1 && value!.HideKeys);
                                    upsertCommand!.AddParameterWithValue("@HideWeapon", playerSlot != -1 && value!.HideWeapon);
                                    upsertCommand!.AddParameterWithValue("@HidePlayers", playerSlot != -1 && value!.HidePlayers);
                                    upsertCommand!.AddParameterWithValue("@SoundsEnabled", playerSlot != -1 && value!.SoundsEnabled);
                                    upsertCommand!.AddParameterWithValue("@PlayerFov", playerSlot == -1 ? 0 : value!.PlayerFov);
                                    upsertCommand!.AddParameterWithValue("@HudType", playerSlot == -1 ? 1 : value!.CurrentHudType);
                                    upsertCommand!.AddParameterWithValue("@IsVip", isVip);
                                    upsertCommand!.AddParameterWithValue("@BigGifID", bigGif);
                                    upsertCommand!.AddParameterWithValue("@GlobalPoints", newPoints);

                                    await upsertCommand!.ExecuteNonQueryAsync();

                                    if (!import) Server.NextFrame(() => GainPointsMessage(playerSlot, playerName, newPoints, playerPoints));

                                    Server.NextFrame(() => SharpTimerDebug($"Set points in database for {playerName} from {playerPoints} to {newPoints}"));
                                }
                                else
                                {
                                    SharpTimerError($"Error setting player points to database for {playerName}: player was not on the server anymore");
                                }
                            }

                        }
                        else
                        {
                            Server.NextFrame(() => SharpTimerDebug($"No player stats yet"));

                            int newPoints = await CalculatePlayerPoints(steamId, playerName, timerTicks, oldTicks, beatPB, bonusX, style, completions, mapname, false) + playerPoints;

                            await row.CloseAsync();

                            string upsertQuery = $@"
                                REPLACE INTO `{PlayerStatsTable}`
                                    (`PlayerName`, `SteamID`, `TimesConnected`, `LastConnected`, `HideTimerHud`, `HideKeys`,
                                    `SoundsEnabled`, `PlayerFov`, `HudType`, `IsVip`, `BigGifID`, `GlobalPoints`,
                                    `HideWeapon`, `HidePlayers`)
                                VALUES
                                    (@PlayerName, @SteamID, @TimesConnected, @LastConnected, @HideTimerHud, @HideKeys,
                                    @SoundsEnabled, @PlayerFov, @HudType, @IsVip, @BigGifID, @GlobalPoints,
                                    @HideWeapon, @HidePlayers);";

                            DbCommand upsertCommand = new MySqlCommand(upsertQuery, (MySqlConnection)connection);

                            using (upsertCommand)
                            {
                                if (playerTimers.TryGetValue(playerSlot, out PlayerTimerInfo? value) || playerSlot == -1)
                                {
                                    upsertCommand!.AddParameterWithValue("@PlayerName", playerName);
                                    upsertCommand!.AddParameterWithValue("@SteamID", steamId);
                                    upsertCommand!.AddParameterWithValue("@TimesConnected", 1);
                                    upsertCommand!.AddParameterWithValue("@LastConnected", timeNowUnix);
                                    upsertCommand!.AddParameterWithValue("@HideTimerHud", playerSlot != -1 && value!.HideTimerHud);
                                    upsertCommand!.AddParameterWithValue("@HideKeys", playerSlot != -1 && value!.HideKeys);
                                    upsertCommand!.AddParameterWithValue("@HideWeapon", playerSlot != -1 && value!.HideWeapon);
                                    upsertCommand!.AddParameterWithValue("@HidePlayers", playerSlot != -1 && value!.HidePlayers);
                                    upsertCommand!.AddParameterWithValue("@SoundsEnabled", playerSlot != -1 && value!.SoundsEnabled);
                                    upsertCommand!.AddParameterWithValue("@PlayerFov", playerSlot == -1 ? 0 : value!.PlayerFov);
                                    upsertCommand!.AddParameterWithValue("@HudType", playerSlot == -1 ? 1 : value!.CurrentHudType);
                                    upsertCommand!.AddParameterWithValue("@IsVip", false);
                                    upsertCommand!.AddParameterWithValue("@BigGifID", "x");
                                    upsertCommand!.AddParameterWithValue("@GlobalPoints", newPoints);

                                    await upsertCommand!.ExecuteNonQueryAsync();

                                    if (!import) Server.NextFrame(() => GainPointsMessage(playerSlot, playerName, newPoints, playerPoints));

                                    Server.NextFrame(() => SharpTimerDebug($"Set points in database for {playerName} from {playerPoints} to {newPoints}"));
                                }
                                else
                                {
                                    SharpTimerError($"Error setting player points to database for {playerName}: player was not on the server anymore");
                                }
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Server.NextFrame(() => SharpTimerError($"Error getting player stats from database for {playerName}: {ex}"));
            }
        }

        private async Task UpsertGlobalPointsAsync(string steamId, string playerName, int points)
        {
            using (var connection = await OpenConnectionAsync())
            {
                await CreatePlayerStatsTableAsync(connection);

                var sql = $@"
                    INSERT INTO `{PlayerStatsTable}` (`PlayerName`, `SteamID`, `GlobalPoints`)
                    VALUES (@PlayerName, @SteamID, @Delta)
                    ON DUPLICATE KEY UPDATE
                    `PlayerName`   = VALUES(`PlayerName`),
                    `GlobalPoints` = IFNULL(`GlobalPoints`, 0) + @Delta;";

                using (var cmd = new MySqlCommand(sql, (MySqlConnection)connection))
                {
                    cmd.Parameters.AddWithValue("@PlayerName", playerName);
                    cmd.Parameters.AddWithValue("@SteamID", steamId);
                    cmd.Parameters.AddWithValue("@Delta", points);
                    await cmd.ExecuteNonQueryAsync();
                }
            }
        }

        public void GainPointsMessage(int playerSlot, string playerName, double newPoints, double playerPoints)
        {
            int delta = Convert.ToInt32(newPoints - playerPoints);
            if (delta == 0) return;

            if (connectedPlayers.TryGetValue(playerSlot, out var player) && IsAllowedPlayer(player))
            {
                player.PrintToChat(Localizer["gained_points", playerName, delta, newPoints]);
            }
        }
    }
}