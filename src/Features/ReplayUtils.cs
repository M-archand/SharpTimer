using CounterStrikeSharp.API;
using CounterStrikeSharp.API.Core;
using CounterStrikeSharp.API.Modules.Utils;
using CounterStrikeSharp.API.Modules.Entities.Constants;
using System.Text.Json;
using Vector = CounterStrikeSharp.API.Modules.Utils.Vector;
using System.Runtime.Serialization.Formatters.Binary;
using System.Data.Common;
using MySqlConnector;
using Npgsql;
using System.Data.SQLite;
using static SharpTimer.PlayerReplays;

namespace SharpTimer
{
    public partial class SharpTimer
    {
        private void ReplayUpdate(CCSPlayerController player, int timerTicks)
        {
            try
            {
                if (!IsAllowedPlayer(player)) return;

                // Get the player's current position and rotation
                ReplayVector currentPosition = ReplayVector.GetVectorish(player.Pawn.Value!.CBodyComponent?.SceneNode?.AbsOrigin ?? new Vector(0, 0, 0));
                ReplayVector currentSpeed = ReplayVector.GetVectorish(player.PlayerPawn.Value!.AbsVelocity ?? new Vector(0, 0, 0));
                ReplayQAngle currentRotation = ReplayQAngle.GetQAngleish(player.PlayerPawn.Value.EyeAngles ?? new QAngle(0, 0, 0));

                var buttons = player.Buttons;
                var flags = player.Pawn.Value.Flags;
                var moveType = player.Pawn.Value.MoveType;

                var ReplayFrame = new ReplayFrames
                {
                    Position = currentPosition,
                    Rotation = currentRotation,
                    Speed = currentSpeed,
                    Buttons = buttons,
                    Flags = flags,
                    MoveType = moveType
                };

                playerReplays[player.Slot].replayFrames.Add(ReplayFrame);
            }
            catch (Exception ex)
            {
                SharpTimerError($"Error in ReplayUpdate: {ex.Message}");
            }
        }

        private void ReplayPlayback(CCSPlayerController player, int plackbackTick)
        {
            try
            {
                if (!IsAllowedPlayer(player)) return;

                //player.LerpTime = 0.0078125f;

                if (playerTimers.TryGetValue(player.Slot, out PlayerTimerInfo? value))
                {
                    
                    var replayFrame = playerReplays[player.Slot].replayFrames[plackbackTick];

                    if (((PlayerFlags)replayFrame.Flags & PlayerFlags.FL_ONGROUND) != 0)
                    {
                        SetMoveType(player, MoveType_t.MOVETYPE_WALK);
                    }
                    else
                    {
                        SetMoveType(player, MoveType_t.MOVETYPE_OBSERVER);
                    }

                    if (((PlayerFlags)replayFrame.Flags & PlayerFlags.FL_DUCKING) != 0)
                    {
                        value.MovementService!.DuckAmount = 1;
                    }
                    else
                    {
                        value.MovementService!.DuckAmount = 0;
                    }

                    player.PlayerPawn.Value!.Teleport(ReplayVector.ToVector(replayFrame.Position!), ReplayQAngle.ToQAngle(replayFrame.Rotation!), ReplayVector.ToVector(replayFrame.Speed!));

                    var replayButtons = $"{((replayFrame.Buttons & PlayerButtons.Moveleft) != 0 ? "A" : "_")} " +
                                        $"{((replayFrame.Buttons & PlayerButtons.Forward) != 0 ? "W" : "_")} " +
                                        $"{((replayFrame.Buttons & PlayerButtons.Moveright) != 0 ? "D" : "_")} " +
                                        $"{((replayFrame.Buttons & PlayerButtons.Back) != 0 ? "S" : "_")} " +
                                        $"{((replayFrame.Buttons & PlayerButtons.Jump) != 0 ? "J" : "_")} " +
                                        $"{((replayFrame.Buttons & PlayerButtons.Duck) != 0 ? "C" : "_")}";

                    if (value.HideKeys != true && value.IsReplaying == true && keysOverlayEnabled == true)
                    {
                        player.PrintToCenter(replayButtons);
                    }
                }
            }
            catch (Exception ex)
            {
                SharpTimerError($"Error in ReplayPlayback: {ex.Message}");
            }
        }

        private void ReplayPlay(CCSPlayerController player)
        {
            try
            {
                int totalFrames = playerReplays[player.Slot].replayFrames.Count;

                if (totalFrames <= 128)
                {
                    OnRecordingStop(player);
                }

                if (playerReplays[player.Slot].CurrentPlaybackFrame < 0 || playerReplays[player.Slot].CurrentPlaybackFrame >= totalFrames)
                {
                    playerReplays[player.Slot].CurrentPlaybackFrame = 0;
                    Action<CCSPlayerController?, float, bool> adjustVelocity = use2DSpeed ? AdjustPlayerVelocity2D : AdjustPlayerVelocity;
                    adjustVelocity(player, 0, false);
                }

                if (jumpStatsEnabled) InvalidateJS(player.Slot);
                ReplayPlayback(player, playerReplays[player.Slot].CurrentPlaybackFrame);

                playerReplays[player.Slot].CurrentPlaybackFrame++;
            }
            catch (Exception ex)
            {
                SharpTimerError($"Error in ReplayPlay: {ex.Message}");
            }
        }

        private void OnRecordingStart(CCSPlayerController player, int bonusX = 0, int style = 0)
        {
            try
            {
                playerReplays.Remove(player.Slot);
                playerReplays[player.Slot] = new PlayerReplays
                {
                    BonusX = bonusX,
                    Style = style
                };
                playerTimers[player.Slot].IsRecordingReplay = true;
            }
            catch (Exception ex)
            {
                SharpTimerError($"Error in OnRecordingStart: {ex.Message}");
            }
        }

        private void OnRecordingStop(CCSPlayerController player)
        {
            try
            {
                playerTimers[player.Slot].IsRecordingReplay = false;
                SetMoveType(player, MoveType_t.MOVETYPE_WALK);
            }
            catch (Exception ex)
            {
                SharpTimerError($"Error in OnRecordingStop: {ex.Message}");
            }
        }

        public async Task DumpReplayToJson(CCSPlayerController player, string steamID, int playerSlot, int bonusX = 0, int style = 0)
        {
            await Task.Run(() =>
            {
                if (!IsAllowedPlayer(player))
                {
                    SharpTimerError($"Error in DumpReplayToJson: Player not allowed or not on server anymore");
                    return;
                }

                string fileName = $"{steamID}_replay.json";
                string playerReplaysDirectory;
                if (style != 0) playerReplaysDirectory = Path.Join(gameDir, "csgo", "cfg", "SharpTimer", "PlayerReplayData", bonusX == 0 ? $"{currentMapName}" : $"{currentMapName}_bonus{bonusX}", GetNamedStyle(style));
                else playerReplaysDirectory = Path.Join(gameDir, "csgo", "cfg", "SharpTimer", "PlayerReplayData", bonusX == 0 ? $"{currentMapName}" : $"{currentMapName}_bonus{bonusX}");
                string playerReplaysPath = Path.Join(playerReplaysDirectory, fileName);

                try
                {
                    if (!Directory.Exists(playerReplaysDirectory))
                    {
                        Directory.CreateDirectory(playerReplaysDirectory);
                    }

                    if (playerReplays[playerSlot].replayFrames.Count >= maxReplayFrames) return;

                    var indexedReplayFrames = playerReplays[playerSlot].replayFrames
                        .Select((frame, index) => new IndexedReplayFrames { Index = index, Frame = frame })
                        .ToList();

                    using (Stream stream = new FileStream(playerReplaysPath, FileMode.Create))
                    {
                        JsonSerializer.Serialize(stream, indexedReplayFrames);
                    }
                }
                catch (Exception ex)
                {
                    SharpTimerError($"Error during serialization: {ex.Message}");
                }
            });
        }
        
        public async Task DumpReplayToBinary(CCSPlayerController player, string steamID, int playerSlot, int bonusX = 0, int style = 0)
        {
            await Task.Run(() =>
            {
                if (!IsAllowedPlayer(player))
                {
                    SharpTimerError($"Error in DumpReplayToBinary: Player not allowed or not on server anymore");
                    return;
                }

                string fileName = $"{steamID}_replay.dat";
                string playerReplaysDirectory;
                if (style != 0) playerReplaysDirectory = Path.Join(gameDir, "csgo", "cfg", "SharpTimer", "PlayerReplayData", bonusX == 0 ? $"{currentMapName}" : $"{currentMapName}_bonus{bonusX}", GetNamedStyle(style));
                else playerReplaysDirectory = Path.Join(gameDir, "csgo", "cfg", "SharpTimer", "PlayerReplayData", bonusX == 0 ? $"{currentMapName}" : $"{currentMapName}_bonus{bonusX}");
                string playerReplaysPath = Path.Join(playerReplaysDirectory, fileName);

                try
                {
                    if (!Directory.Exists(playerReplaysDirectory))
                    {
                        Directory.CreateDirectory(playerReplaysDirectory);
                    }

                    if (playerReplays[playerSlot].replayFrames.Count >= maxReplayFrames) return;

                    var indexedReplayFrames = playerReplays[playerSlot].replayFrames
                        .Select((frame, index) => new IndexedReplayFrames { Index = index, Frame = frame })
                        .ToList();

                    using Stream stream = new FileStream(playerReplaysPath, FileMode.Create);
                    BinaryWriter writer = new BinaryWriter(stream);
                    
                    writer.Write(REPLAY_VERSION);
                    
                    foreach (var frame in indexedReplayFrames)
                    {
                        writer.Write(frame.Frame!.Position!.X);
                        writer.Write(frame.Frame.Position!.Y);
                        writer.Write(frame.Frame.Position!.Z);
                        writer.Write(frame.Frame.Rotation!.Pitch);
                        writer.Write(frame.Frame.Rotation!.Yaw);
                        writer.Write(frame.Frame.Rotation!.Roll);
                        writer.Write(frame.Frame.Speed!.X);
                        writer.Write(frame.Frame.Speed!.Y);
                        writer.Write(frame.Frame.Speed!.Z);
                        writer.Write((int)frame.Frame.Buttons!);
                        writer.Write((int)frame.Frame.Flags);
                        writer.Write((int)frame.Frame.MoveType);
                    }
                }
                catch (Exception ex)
                {
                    SharpTimerError($"Error during serialization: {ex.Message}");
                }
            });
        }

        public async Task<string> GetReplayJson(CCSPlayerController player, int playerSlot)
        {
            if (!IsAllowedPlayer(player))
            {
                SharpTimerError($"Error in GetReplayJson: Player not allowed or not on server anymore");
                return "";
            }

            try
            {
                if (playerReplays[playerSlot].replayFrames.Count >= maxReplayFrames) 
                    return "";

                var indexedReplayFrames = playerReplays[playerSlot].replayFrames
                    .Select((frame, index) => new IndexedReplayFrames { Index = index, Frame = frame })
                    .ToList();

                return await Task.Run(() => JsonSerializer.Serialize(indexedReplayFrames));
            }
            catch (Exception ex)
            {
                SharpTimerError($"Error during serialization: {ex.Message}");
                return "";
            }
        }

        private async Task ReadReplayFromJson(CCSPlayerController player, string steamId, int playerSlot, int bonusX = 0, int style = 0)
        {
            SharpTimerDebug($"Reading replay from JSON");
            string fileName = $"{steamId}_replay.json";
            string playerReplaysPath;
            if (style != 0) playerReplaysPath = Path.Join(gameDir, "csgo", "cfg", "SharpTimer", "PlayerReplayData", bonusX == 0 ? currentMapName : $"{currentMapName}_bonus{bonusX}", GetNamedStyle(style), fileName);
            else playerReplaysPath = Path.Join(gameDir, "csgo", "cfg", "SharpTimer", "PlayerReplayData", bonusX == 0 ? currentMapName : $"{currentMapName}_bonus{bonusX}", fileName);

            try
            {
                if (File.Exists(playerReplaysPath))
                {
                    SharpTimerDebug($"Path: {playerReplaysPath}, creating stream");
                    var jsonString = await File.ReadAllTextAsync(playerReplaysPath);
                    if (!jsonString.Contains("PositionString"))
                    {
                        var indexedReplayFrames = JsonSerializer.Deserialize<List<IndexedReplayFrames>>(jsonString);

                        if (indexedReplayFrames != null)
                        {
                            var replayFrames = indexedReplayFrames
                                .OrderBy(frame => frame.Index)
                                .Select(frame => frame.Frame)
                                .ToList();

                            if (!playerReplays.TryGetValue(playerSlot, out PlayerReplays? value))
                            {
                                value = new PlayerReplays();
                                playerReplays[playerSlot] = value;
                            }

                            value.replayFrames = replayFrames!;
                        }
                    }
                    else
                    {
                        Server.NextFrame(() => { PrintToChat(player, $"Unsupported replay format"); });
                    }
                }
                else
                {
                    SharpTimerError($"File does not exist: {playerReplaysPath}");
                    Server.NextFrame(() => PrintToChat(player, Localizer["replay_dont_exist"]));
                }
            }
            catch (Exception ex)
            {
                SharpTimerError($"Error during deserialization: {ex.Message}");
                SharpTimerError($"Error during deserialization: {ex.StackTrace}");
            }
        }

        private async Task ReadReplayFromBinary(CCSPlayerController player, string steamId, int playerSlot, int bonusX = 0, int style = 0)
        {
            SharpTimerDebug($"Reading replay from Binary");
            string fileName = $"{steamId}_replay.dat";
            string playerReplaysPath;
            if (style != 0) playerReplaysPath = Path.Join(gameDir, "csgo", "cfg", "SharpTimer", "PlayerReplayData", bonusX == 0 ? currentMapName : $"{currentMapName}_bonus{bonusX}", GetNamedStyle(style), fileName);
            else playerReplaysPath = Path.Join(gameDir, "csgo", "cfg", "SharpTimer", "PlayerReplayData", bonusX == 0 ? currentMapName : $"{currentMapName}_bonus{bonusX}", fileName);

            try
            {
                if (!File.Exists(playerReplaysPath))
                {
                    SharpTimerError($"File does not exist: {playerReplaysPath}");
                    Server.NextFrame(() => PrintToChat(player, Localizer["replay_dont_exist"]));
                    return;
                }
                
                SharpTimerDebug($"Path: {playerReplaysPath}, creating stream");
                
                using Stream stream = new FileStream(playerReplaysPath, FileMode.Open);
                BinaryReader reader = new BinaryReader(stream);
                
                var version = reader.ReadInt32();
                if (version != REPLAY_VERSION)
                {
                    SharpTimerError($"Unsupported replay version: {version}");
                    Server.NextFrame(() => PrintToChat(player, $"Unsupported replay version: {version}"));
                    return;
                }
                   
                var replayFrames = new List<PlayerReplays.ReplayFrames>();
                
                await Server.NextFrameAsync(() => {
                    while (reader.BaseStream.Position != reader.BaseStream.Length)
                    {
                            var position = new Vector(reader.ReadSingle(), reader.ReadSingle(), reader.ReadSingle());
                            var rotation = new QAngle(reader.ReadSingle(), reader.ReadSingle(), reader.ReadSingle());
                            var speed = new Vector(reader.ReadSingle(), reader.ReadSingle(), reader.ReadSingle());
                            var buttons = (PlayerButtons)reader.ReadInt32();
                            var flags = (uint)reader.ReadInt32();
                            var moveType = (MoveType_t)reader.ReadInt32();
                        
                            replayFrames.Add(new PlayerReplays.ReplayFrames
                            {
                                Position = ReplayVector.GetVectorish(position),
                                Rotation = ReplayQAngle.GetQAngleish(rotation),
                                Speed    = ReplayVector.GetVectorish(speed),
                                Buttons  = buttons,
                                Flags    = flags,
                                MoveType = moveType
                            });
                    }
                });
                
                if (!playerReplays.TryGetValue(playerSlot, out PlayerReplays? value))
                {
                    value = new PlayerReplays();
                    playerReplays[playerSlot] = value;
                }
                
                value.replayFrames = replayFrames;
            }
            catch (Exception ex)
            {
                SharpTimerError($"Error during deserialization: {ex.Message}");
                SharpTimerError($"Error during deserialization: {ex.StackTrace}");
            }
        }

        public static byte[] GetReplayBinaryData(PlayerReplays replay)
        {
            const int REPLAY_VERSION = 1;
            using (var ms = new MemoryStream())
            {
                using (var writer = new BinaryWriter(ms))
                {
                    writer.Write(REPLAY_VERSION);
                    foreach (var frame in replay.replayFrames)
                    {
                        if (frame.Position != null && frame.Rotation != null && frame.Speed != null)
                        {
                            writer.Write(frame.Position.X);
                            writer.Write(frame.Position.Y);
                            writer.Write(frame.Position.Z);
                            
                            writer.Write(frame.Rotation.Pitch);
                            writer.Write(frame.Rotation.Yaw);
                            writer.Write(frame.Rotation.Roll);
                            
                            writer.Write(frame.Speed.X);
                            writer.Write(frame.Speed.Y);
                            writer.Write(frame.Speed.Z);
                            
                            writer.Write((int)(frame.Buttons ?? default(PlayerButtons)));
                            writer.Write((int)frame.Flags);
                            writer.Write((int)frame.MoveType);
                        }
                    }
                    writer.Flush();
                    return ms.ToArray();
                }
            }
        }

        public async Task ReadReplayFromDatabase(CCSPlayerController player, string steamId, int playerSlot, int bonusX, int style)
        {
            // Determine the map name—if bonusX is non-zero, append bonus info.
            string mapName = bonusX == 0 ? currentMapName! : $"{currentMapName}_bonus{bonusX}";
            SharpTimerDebug($"ReadReplayFromDatabase was called & Computed map name for replay: {mapName}");
            
            try
            {
                using (var connection = await OpenConnectionAsync())
                {
                    DbCommand command = null!;
                    string query = string.Empty;
                    switch (dbType)
                    {
                        case DatabaseType.MySQL:
                            query = "SELECT ReplayData FROM PlayerReplays WHERE SteamID = @SteamID AND MapName = @MapName AND Style = @Style";
                            command = new MySqlCommand(query, (MySqlConnection)connection);
                            break;
                        case DatabaseType.PostgreSQL:
                            query = @"SELECT ""ReplayData"" FROM ""PlayerReplays"" WHERE ""SteamID"" = @SteamID AND ""MapName"" = @MapName AND ""Style"" = @Style";
                            command = new NpgsqlCommand(query, (NpgsqlConnection)connection);
                            break;
                        case DatabaseType.SQLite:
                            query = "SELECT ReplayData FROM PlayerReplays WHERE SteamID = @SteamID AND MapName = @MapName AND Style = @Style";
                            command = new SQLiteCommand(query, (SQLiteConnection)connection);
                            break;
                        default:
                            throw new Exception("Unsupported database type");
                    }
                    
                    // Set parameters using extension method
                    command.AddParameterWithValue("@SteamID", steamId);
                    command.AddParameterWithValue("@MapName", mapName);
                    command.AddParameterWithValue("@Style", style);
                    
                    // Execute the query to get the replay data blob.
                    object? result = await command.ExecuteScalarAsync();
                    if (result == null || result == DBNull.Value)
                    {
                        SharpTimerError($"No replay data found for SteamID: {steamId}, Map: {mapName}");
                        return;
                    }
                    
                    byte[] replayBinaryData = (byte[])result;
                    
                    // Create a new PlayerReplays instance to load the frames
                    PlayerReplays loadedReplay = new PlayerReplays();
                    
                    using (var ms = new MemoryStream(replayBinaryData))
                    {
                        using (var reader = new BinaryReader(ms))
                        {
                            // Read the replay version
                            int version = reader.ReadInt32();
                        
                            // Loop while there is data available
                            while (ms.Position < ms.Length)
                            {
                                // Read position
                                ReplayVector position = new ReplayVector(reader.ReadSingle(), reader.ReadSingle(), reader.ReadSingle());

                                // Read rotation
                                ReplayQAngle rotation = new ReplayQAngle(reader.ReadSingle(), reader.ReadSingle(), reader.ReadSingle());

                                // Read speed
                                ReplayVector speed = new ReplayVector(reader.ReadSingle(), reader.ReadSingle(), reader.ReadSingle());
                                
                                // Read buttons, flags, and move type
                                PlayerButtons buttons = (PlayerButtons)reader.ReadInt32();
                                uint flags = (uint)reader.ReadInt32();
                                MoveType_t moveType = (MoveType_t)reader.ReadInt32();

                                // Create a new frame and add it to the replay
                                ReplayFrames frame = new ReplayFrames
                                {
                                    Position = position,
                                    Rotation = rotation,
                                    Speed = speed,
                                    Buttons = buttons,
                                    Flags = flags,
                                    MoveType = moveType
                                };
                                
                                loadedReplay.replayFrames.Add(frame);
                            }
                        }
                    }
                    
                    // Assign the loaded replay to the player's replay dictionary for playback
                    playerReplays[playerSlot] = loadedReplay;
                }
            }
            catch (Exception ex)
            {
                SharpTimerError($"Error reading replay from database: {ex.Message}");
            }
        }

        public async Task DumpReplayToDatabase(CCSPlayerController player, string steamId, int playerSlot, int bonusX, int style)
        {
            // Obtain the binary replay data
            byte[] replayBinaryData = GetReplayBinaryData(playerReplays[playerSlot]);
            SharpTimerDebug($"DumpReplayToDatabase called with SteamID: {steamId}, bonusX: {bonusX}, style: {style}, currentMapName: {currentMapName}");
            SharpTimerDebug($"Replay binary data length: {replayBinaryData.Length}");

            SharpTimerDebug($"Replay binary data length: {replayBinaryData.Length}");
            
            
            try
            {
                using (var connection = await OpenConnectionAsync())
                {
                    // Ensure the PlayerReplays table exists
                    await CreatePlayerReplaysTableAsync(connection);
                    
                    DbCommand command = null!;
                    string query = string.Empty;
                    switch (dbType)
                    {
                        case DatabaseType.MySQL:
                            query = @"
                                INSERT INTO PlayerReplays (SteamID, MapName, Style, ReplayData)
                                VALUES (@SteamID, @MapName, @Style, @ReplayData)
                                ON DUPLICATE KEY UPDATE ReplayData = @ReplayData";
                            command = new MySqlCommand(query, (MySqlConnection)connection);
                            break;
                        case DatabaseType.PostgreSQL:
                            query = @"
                                INSERT INTO ""PlayerReplays"" (""SteamID"", ""MapName"", ""Style"", ""ReplayData"")
                                VALUES (@SteamID, @MapName, @Style, @ReplayData)
                                ON CONFLICT (""SteamID"", ""MapName"", ""Style"") DO UPDATE SET ReplayData = @ReplayData";
                            command = new NpgsqlCommand(query, (NpgsqlConnection)connection);
                            break;
                        case DatabaseType.SQLite:
                            query = @"
                                INSERT OR REPLACE INTO PlayerReplays (SteamID, MapName, Style, ReplayData)
                                VALUES (@SteamID, @MapName, @Style, @ReplayData)";
                            command = new SQLiteCommand(query, (SQLiteConnection)connection);
                            break;
                        default:
                            throw new Exception("Unsupported database type");
                    }
                    
                    // Set parameters
                    command.AddParameterWithValue("@SteamID", steamId);
                    string mapName = bonusX == 0 ? currentMapName! : $"{currentMapName}_bonus{bonusX}";
                    SharpTimerDebug($"ReadReplayFromDatabase was called & Computed map name for replay: {mapName}");
                    command.AddParameterWithValue("@MapName", mapName);
                    command.AddParameterWithValue("@Style", style);
                    command.AddParameterWithValue("@ReplayData", replayBinaryData);
                    
                    await command.ExecuteNonQueryAsync();
                }
            }
            catch (Exception ex)
            {
                SharpTimerError($"Error saving replay to database: {ex.Message}");
            }
        }

        /*
        private async Task ReadReplayFromGlobal(CCSPlayerController player, int recordId, int style, int bonusX = 0)
        {
            string currentMapFull = bonusX == 0 ? currentMapName! : $"{currentMapName}_bonus{bonusX}";
            var payload = new
            {
                record_id = recordId,
                map_name = currentMapFull,
                style = style
            };

            try
            {

                var jsonString = await GetReplayFromGlobal(payload);
                var indexedReplayFrames = JsonSerializer.Deserialize<List<IndexedReplayFrames>>(jsonString);

                if (indexedReplayFrames != null)
                {
                    var replayFrames = indexedReplayFrames
                        .OrderBy(frame => frame.Index)
                        .Select(frame => frame.Frame)
                        .ToList();

                    if (!playerReplays.TryGetValue(player.Slot, out PlayerReplays? value))
                    {
                        value = new PlayerReplays();
                        playerReplays[player.Slot] = value;
                    }

                    value.replayFrames = replayFrames!;
                }
            }
            catch (Exception ex)
            {
                SharpTimerError($"Error during deserialization: {ex.Message}");
            }
        }
        */

        private async Task SpawnReplayBot()
        {
            try
            {
                if (await CheckSRReplay() != true) return;

                Server.NextFrame(() =>
                {
                    startKickingAllFuckingBotsExceptReplayOneIFuckingHateValveDogshitFuckingCompanySmile = false;
                    foreach (CCSPlayerController bot in connectedReplayBots.Values.ToList())
                    {
                        if (bot != null)
                        {
                            OnPlayerDisconnect(bot, true);
                            if (connectedReplayBots.TryGetValue(bot.Slot, out var someValue)) connectedReplayBots.Remove(bot.Slot);
                        }
                    }
                    Server.ExecuteCommand("sv_cheats 1");
                    Server.ExecuteCommand("bot_add_ct");
                    Server.ExecuteCommand("bot_quota 1");
                    Server.ExecuteCommand("bot_quota_mode 0");
                    Server.ExecuteCommand("bot_stop 1");
                    Server.ExecuteCommand("bot_freeze 1");
                    Server.ExecuteCommand("bot_zombie 1");
                    Server.ExecuteCommand("bot_chatter off");
                    Server.ExecuteCommand("sv_cheats 0");

                    AddTimer(3.0f, () =>
                    {
                        foundReplayBot = false;
                        SharpTimerDebug($"Trying to find replay bot!");
                        var playerEntities = Utilities.FindAllEntitiesByDesignerName<CCSPlayerController>("cs_player_controller");
                        foreach (var tempPlayer in playerEntities)
                        {
                            if (tempPlayer == null || !tempPlayer.IsValid || !tempPlayer.IsBot || tempPlayer.IsHLTV)
                                continue;
                            if (tempPlayer.UserId.HasValue)
                            {
                                if (foundReplayBot == true)
                                {
                                    OnPlayerDisconnect(tempPlayer, true);
                                    Server.ExecuteCommand($"kickid {tempPlayer.Slot}");
                                    SharpTimerDebug($"Kicking unused replay bot!");
                                }
                                else
                                {
                                    SharpTimerDebug($"Found replay bot!");
                                    OnReplayBotConnect(tempPlayer);
                                    tempPlayer.PlayerPawn.Value!.Bot!.IsSleeping = true;
                                    tempPlayer.PlayerPawn.Value!.Bot!.AllowActive = true;
                                    tempPlayer.RemoveWeapons();
                                    tempPlayer!.Pawn.Value!.Collision.CollisionAttribute.CollisionGroup = (byte)CollisionGroup.COLLISION_GROUP_DISSOLVING;
                                    tempPlayer!.Pawn.Value!.Collision.CollisionGroup = (byte)CollisionGroup.COLLISION_GROUP_DISSOLVING;
                                    Utilities.SetStateChanged(tempPlayer, "CCollisionProperty", "m_CollisionGroup");
                                    Utilities.SetStateChanged(tempPlayer, "CCollisionProperty", "m_collisionAttribute");
                                    SharpTimerDebug($"Removed Collison for replay bot!");
                                    foundReplayBot = true;
                                    startKickingAllFuckingBotsExceptReplayOneIFuckingHateValveDogshitFuckingCompanySmile = true;
                                }
                            }
                        }
                    });
                });
            }
            catch (Exception ex)
            {
                SharpTimerError($"Error in SpawnReplayBot: {ex.Message}");
            }
        }

        private void OnReplayBotConnect(CCSPlayerController bot)
        {
            try
            {
                var botSlot = bot.Slot;
                var botName = bot.PlayerName;
                
                if (bot.IsHLTV)
                    return;

                AddTimer(3.0f, () =>
                {
                    OnPlayerConnect(bot, true);
                    connectedReplayBots[botSlot] = new CCSPlayerController(bot.Handle);
                    ChangePlayerName(bot, replayBotName);
                    playerTimers[botSlot].IsTimerBlocked = true;
                    _ = Task.Run(async () => await ReplayHandler(bot, botSlot));
                    SharpTimerDebug($"Starting replay for {botName}");
                });
            }
            catch (Exception ex)
            {
                SharpTimerError($"Error in OnReplayBotConnect: {ex.Message}");
            }
        }


        public async Task<bool> CheckSRReplay(string topSteamID = "x", int bonusX = 0, int style = 0)
        {
            var (srSteamID, srPlayerName, srTime) = ("null", "null", "null");

            if (enableDb)
            {
                (srSteamID, srPlayerName, srTime) = await GetMapRecordSteamIDFromDatabase(bonusX);
            }
            else
            {
                (srSteamID, srPlayerName, srTime) = await GetMapRecordSteamID(bonusX);
            }

            if ((srSteamID == "null" || srPlayerName == "null" || srTime == "null") && topSteamID != "x") return false;

            string ext = useBinaryReplays ? "dat" : "json";
            string fileName = $"{(topSteamID == "x" ? $"{srSteamID}" : $"{topSteamID}")}_replay.{ext}";
            string playerReplaysPath;
            if (style != 0) playerReplaysPath = Path.Join(gameDir, "csgo", "cfg", "SharpTimer", "PlayerReplayData", (bonusX == 0 ? currentMapName : $"{currentMapName}_bonus{bonusX}"), GetNamedStyle(style), fileName);
            else playerReplaysPath = Path.Join(gameDir, "csgo", "cfg", "SharpTimer", "PlayerReplayData", (bonusX == 0 ? currentMapName : $"{currentMapName}_bonus{bonusX}"), fileName);

            try
            {
                if (File.Exists(playerReplaysPath)) {
                    if (useBinaryReplays) {
                        var reader = new BinaryReader(File.Open(playerReplaysPath, FileMode.Open));
                        var version = reader.ReadInt32();
                        
                        return version == REPLAY_VERSION;
                    }
                    var jsonString = await File.ReadAllTextAsync(playerReplaysPath);
                    if (!jsonString.Contains("PositionString"))
                    {
                        var indexedReplayFrames = JsonSerializer.Deserialize<List<IndexedReplayFrames>>(jsonString);

                        if (indexedReplayFrames != null)
                        {
                            return true;
                        }
                        return false;
                    }
                    else
                    {
                        return false;
                    }
                }
                else
                {
                    return false;
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error during deserialization: {ex.Message}");
                return false;
            }
        }
    }
}