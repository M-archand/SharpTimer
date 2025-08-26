using System.Text.Json;
using MySqlConnector;
using CounterStrikeSharp.API;
using CounterStrikeSharp.API.Core;
using CounterStrikeSharp.API.Core.Attributes.Registration;
using CounterStrikeSharp.API.Modules.Admin;
using CounterStrikeSharp.API.Modules.Commands;
using CounterStrikeSharp.API.Modules.Utils;
using System.Data;
using Npgsql;
using System.Data.Common;
using System.Text.RegularExpressions;

namespace SharpTimer
{
    public enum DatabaseType
    {
        MySQL,
        PostgreSQL
    }
    partial class SharpTimer
    {
        public async Task<IDbConnection> OpenConnectionAsync()
        {
            IDbConnection? connection = null;
            switch (dbType)
            {
                case DatabaseType.MySQL:
                    connection = new MySqlConnection(await GetConnectionStringFromConfigFile());
                    await (connection as MySqlConnection)!.OpenAsync();
                    break;
                case DatabaseType.PostgreSQL:
                    connection = new NpgsqlConnection(await GetConnectionStringFromConfigFile());
                    await (connection as NpgsqlConnection)!.OpenAsync();
                    break;
            }
            if (connection!.State != ConnectionState.Open)
            {
                useMySQL = false;
                usePostgres = false;
                enableDb = false;
            }
            return connection;
        }
        public IDbConnection OpenConnection()
        {
            IDbConnection? connection = null;
            switch (dbType)
            {
                case DatabaseType.MySQL:
                    connection = new MySqlConnection(GetConnectionStringOnMainThread());
                    (connection as MySqlConnection)!.Open();
                    break;
                case DatabaseType.PostgreSQL:
                    connection = new NpgsqlConnection(GetConnectionStringOnMainThread());
                    (connection as NpgsqlConnection)!.Open();
                    break;
            }
            if (connection!.State != ConnectionState.Open)
            {
                useMySQL = false;
                usePostgres = false;
                enableDb = false;
            }
            return connection;
        }
        private string GetConnectionStringOnMainThread()
        {
            try
            {
                using (JsonDocument? jsonConfig = LoadJsonOnMainThread(dbPath)!)
                {
                    if (jsonConfig != null)
                    {
                        JsonElement root = jsonConfig.RootElement;

                        string host = root.TryGetProperty("Host", out var hostProperty) ? hostProperty.GetString()! : "localhost";
                        string database = root.TryGetProperty("Database", out var databaseProperty) ? databaseProperty.GetString()! : "database";
                        string username = root.TryGetProperty("Username", out var usernameProperty) ? usernameProperty.GetString()! : "root";
                        string password = root.TryGetProperty("Password", out var passwordProperty) ? passwordProperty.GetString()! : "root";
                        int port = root.TryGetProperty("Port", out var portProperty) ? portProperty.GetInt32()! : 3306;
                        string tableprefix = root.TryGetProperty("TablePrefix", out var tableprefixProperty) ? tableprefixProperty.GetString()! : "";

                        PlayerStatsTable = $"{(tableprefix != "" ? $"PlayerStats_{tableprefix}" : "PlayerStats")}";

                        if (dbType.Equals(DatabaseType.MySQL))
                        {
                            int timeout = root.TryGetProperty("Timeout", out var timeoutProperty) ? timeoutProperty.GetInt32()! : 30;
                            return $"Server={host};Database={database};User ID={username};Password={password};Port={port};CharSet=utf8mb4;Connection Timeout={timeout};";
                        }
                        else if (dbType.Equals(DatabaseType.PostgreSQL))
                        {
                            return $"Server={host};Database={database};User ID={username};Password={password};Port={port};SslMode=Disable";
                        }
                        else
                        {
                            SharpTimerError($"Database type not supported");
                        }
                    }
                    else
                    {
                        SharpTimerError($"Database json was null");
                    }
                }
            }
            catch (Exception ex)
            {
                SharpTimerError($"Error in GetConnectionString: {ex.Message}");
            }
            return "Server=localhost;Database=database;User ID=root;Password=root;Port=3306;";
        }
        private async Task<string> GetConnectionStringFromConfigFile()
        {
            try
            {
                using (JsonDocument? jsonConfig = await LoadJson(dbPath)!)
                {
                    if (jsonConfig != null)
                    {
                        JsonElement root = jsonConfig.RootElement;

                        string host = root.TryGetProperty("Host", out var hostProperty) ? hostProperty.GetString()! : "localhost";
                        string database = root.TryGetProperty("Database", out var databaseProperty) ? databaseProperty.GetString()! : "database";
                        string username = root.TryGetProperty("Username", out var usernameProperty) ? usernameProperty.GetString()! : "root";
                        string password = root.TryGetProperty("Password", out var passwordProperty) ? passwordProperty.GetString()! : "root";
                        int port = root.TryGetProperty("Port", out var portProperty) ? portProperty.GetInt32()! : 3306;
                        string tableprefix = root.TryGetProperty("TablePrefix", out var tableprefixProperty) ? tableprefixProperty.GetString()! : "";

                        PlayerStatsTable = $"{(tableprefix != "" ? $"PlayerStats_{tableprefix}" : "PlayerStats")}";

                        if (dbType.Equals(DatabaseType.MySQL))
                        {
                            int timeout = root.TryGetProperty("Timeout", out var timeoutProperty) ? timeoutProperty.GetInt32()! : 30;
                            return $"Server={host};Database={database};User ID={username};Password={password};Port={port};CharSet=utf8mb4;Connection Timeout={timeout};";
                        }
                        else if (dbType.Equals(DatabaseType.PostgreSQL))
                        {
                            return $"Server={host};Database={database};User ID={username};Password={password};Port={port};SslMode=Disable";
                        }
                        else
                        {
                            SharpTimerError($"Database type not supported");
                        }
                    }
                    else
                    {
                        SharpTimerError($"Database json was null");
                    }
                }
            }
            catch (Exception ex)
            {
                SharpTimerError($"Error in GetConnectionString: {ex.Message}");
            }
            return "Server=localhost;Database=database;User ID=root;Password=root;Port=3306;";
        }

        public async Task CheckTablesAsync()
        {
            string[]? playerRecords;
            string[]? playerStats;
            switch (dbType)
            {
                case DatabaseType.MySQL:
                    playerRecords = [       "MapName VARCHAR(255) DEFAULT ''",
                                                    "SteamID VARCHAR(20) DEFAULT ''",
                                                    "PlayerName VARCHAR(32) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT ''",
                                                    "TimerTicks INT DEFAULT 0",
                                                    "FormattedTime VARCHAR(255) DEFAULT ''",
                                                    "UnixStamp INT DEFAULT 0",
                                                    "LastFinished INT DEFAULT 0",
                                                    "TimesFinished INT DEFAULT 0",
                                                    "Style INT DEFAULT 0"
                                                ];
                    playerStats = [         "SteamID VARCHAR(20) DEFAULT ''",
                                                    "PlayerName VARCHAR(32) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT ''",
                                                    "TimesConnected INT DEFAULT 0",
                                                    "LastConnected INT DEFAULT 0",
                                                    "GlobalPoints INT DEFAULT 0",
                                                    "HideTimerHud BOOL DEFAULT false",
                                                    "HideKeys BOOL DEFAULT false",
                                                    "SoundsEnabled BOOL DEFAULT false",
                                                    "PlayerFov INT DEFAULT 0",
                                                    "HudType INT DEFAULT 1",
                                                    "IsVip BOOL DEFAULT false",
                                                    "BigGifID VARCHAR(16) DEFAULT 'x'"
                                                ];
                    break;
                case DatabaseType.PostgreSQL:
                    playerRecords = [       @"""MapName"" VARCHAR(255) DEFAULT ''",
                                                    @"""SteamID"" VARCHAR(20) DEFAULT ''",
                                                    @"""PlayerName"" VARCHAR(32) DEFAULT ''",
                                                    @"""TimerTicks"" INT DEFAULT 0",
                                                    @"""FormattedTime"" VARCHAR(255) DEFAULT ''",
                                                    @"""UnixStamp"" INT DEFAULT 0",
                                                    @"""LastFinished"" INT DEFAULT 0",
                                                    @"""TimesFinished"" INT DEFAULT 0",
                                                    @"""Style"" INT DEFAULT 0"
                                                ];
                    playerStats = [         @"""SteamID"" VARCHAR(20) DEFAULT ''",
                                                    @"""PlayerName"" VARCHAR(32) DEFAULT ''",
                                                    @"""TimesConnected"" INT DEFAULT 0",
                                                    @"""LastConnected"" INT DEFAULT 0",
                                                    @"""GlobalPoints"" INT DEFAULT 0",
                                                    @"""HideTimerHud"" BOOL DEFAULT false",
                                                    @"""HideKeys"" BOOL DEFAULT false",
                                                    @"""SoundsEnabled"" BOOL DEFAULT false",
                                                    @"""PlayerFov"" INT DEFAULT 0",
                                                    @"""IsVip"" BOOL DEFAULT false",
                                                    @"""BigGifID"" VARCHAR(16) DEFAULT 'x'"
                                                ];
                    break;
                default:
                    playerRecords = null;
                    playerStats = null;
                    SharpTimerError($"Database type not supported");
                    break;
            }
            using (var connection = await OpenConnectionAsync())
            {
                try
                {
                    // Check PlayerRecords
                    SharpTimerDebug($"Checking PlayerRecords Table...");
                    await CreatePlayerRecordsTableAsync(connection);
                    await UpdateTableColumnsAsync(connection, "PlayerRecords", playerRecords!);

                    // Check PlayerStats
                    SharpTimerDebug($"Checking PlayerStats Table...");
                    await CreatePlayerStatsTableAsync(connection);
                    await UpdateTableColumnsAsync(connection, $"{PlayerStatsTable}", playerStats!);
                }
                catch (Exception ex)
                {
                    SharpTimerError($"Error in CheckTablesAsync: {ex}");
                }
            }
        }
        private async Task CreatePlayerRecordsTableAsync(IDbConnection connection)
        {
            DbCommand? createTableCommand;
            string createTableQuery;
            switch (dbType)
            {
                case DatabaseType.MySQL:
                    createTableQuery = @"CREATE TABLE IF NOT EXISTS PlayerRecords (
                                            MapName VARCHAR(255),
                                            SteamID VARCHAR(20),
                                            PlayerName VARCHAR(32) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
                                            TimerTicks INT,
                                            FormattedTime VARCHAR(255),
                                            UnixStamp INT,
                                            TimesFinished INT,
                                            LastFinished INT,
                                            Style INT,
                                            PRIMARY KEY (MapName, SteamID, Style)
                                        )";
                    createTableCommand = new MySqlCommand(createTableQuery, (MySqlConnection)connection);
                    break;
                case DatabaseType.PostgreSQL:
                    createTableQuery = @"CREATE TABLE IF NOT EXISTS ""PlayerRecords"" (
                                            ""MapName"" VARCHAR(255),
                                            ""SteamID"" VARCHAR(20),
                                            ""PlayerName"" VARCHAR(32),
                                            ""TimerTicks"" INT,
                                            ""FormattedTime"" VARCHAR(255),
                                            ""UnixStamp"" INT,
                                            ""TimesFinished"" INT,
                                            ""LastFinished"" INT,
                                            ""Style"" INT,
                                            PRIMARY KEY (""MapName"", ""SteamID"", ""Style"")
                                        )";
                    createTableCommand = new NpgsqlCommand(createTableQuery, (NpgsqlConnection)connection);
                    break;
                default:
                    createTableCommand = null;
                    break;
            }
            using (createTableCommand)
            {
                try
                {
                    await createTableCommand!.ExecuteNonQueryAsync();
                }
                catch (Exception ex)
                {
                    SharpTimerError($"Error in CreatePlayerRecordsTableAsync: {ex.Message}");
                }
            }
        }
        private async Task UpdateTableColumnsAsync(IDbConnection connection, string tableName, string[] columns)
        {
            if (await TableExistsAsync(connection, tableName))
            {
                foreach (string columnDefinition in columns)
                {
                    string columnName = columnDefinition.Split(' ')[0];
                    if (!await ColumnExistsAsync(connection, tableName, columnName))
                    {
                        SharpTimerDebug($"Adding column {columnName} to {tableName}...");
                        await AddColumnToTableAsync(connection, tableName, columnDefinition);
                    }
                }
            }
        }
        private async Task<bool> TableExistsAsync(IDbConnection connection, string tableName)
        {
            DbCommand? command;
            string query;
            switch (dbType)
            {
                case DatabaseType.MySQL:
                    query = $"SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = '{connection.Database}' AND table_name = '{tableName}'";
                    command = new MySqlCommand(query, (MySqlConnection)connection);
                    break;
                case DatabaseType.PostgreSQL:
                    query = $@"SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public' AND table_name = '""{tableName}""'";
                    command = new NpgsqlCommand(query, (NpgsqlConnection)connection);
                    break;
                default:
                    command = null;
                    break;
            }
            using (command)
            {
                try
                {
                    int count = Convert.ToInt32(await command!.ExecuteScalarAsync());
                    return count > 0;
                }
                catch (Exception ex)
                {
                    SharpTimerError($"Error in TableExistsAsync: {ex.Message}");
                    return false;
                }
            }
        }
        private async Task<bool> ColumnExistsAsync(IDbConnection connection, string tableName, string columnName)
        {
            DbCommand? command;
            string query;
            switch (dbType)
            {
                case DatabaseType.MySQL:
                    query = $"SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = '{connection.Database}' AND table_name = '{tableName}' AND column_name = '{columnName}'";
                    command = new MySqlCommand(query, (MySqlConnection)connection);
                    break;
                case DatabaseType.PostgreSQL:
                    query = $@"SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = 'public' AND table_name = '{tableName}' AND column_name = '{columnName}'";
                    command = new NpgsqlCommand(query, (NpgsqlConnection)connection);
                    break;
                default:
                    command = null;
                    break;
            }
            using (command)
            {
                try
                {
                    int count = Convert.ToInt32(await command!.ExecuteScalarAsync());
                    return count > 0;
                }
                catch (Exception ex)
                {
                    SharpTimerError($"Error in ColumnExistsAsync: {ex.Message}");
                    return false;
                }
            }
        }

        private async Task AddColumnToTableAsync(IDbConnection connection, string tableName, string columnDefinition)
        {
            DbCommand? command;
            string query;
            switch (dbType)
            {
                case DatabaseType.MySQL:
                    query = $"ALTER TABLE {tableName} ADD COLUMN {columnDefinition}";
                    command = new MySqlCommand(query, (MySqlConnection)connection);
                    break;
                case DatabaseType.PostgreSQL:
                    query = $@"ALTER TABLE ""{tableName}"" ADD ""{columnDefinition}""";
                    command = new NpgsqlCommand(query, (NpgsqlConnection)connection);
                    break;
                default:
                    command = null;
                    break;
            }
            using (command)
            {
                try
                {
                    await command!.ExecuteNonQueryAsync();
                }
                catch (Exception ex)
                {
                    SharpTimerError($"Error in AddColumnToTableAsync: {ex.Message}");
                }
            }
        }
        private async Task CreatePlayerStatsTableAsync(IDbConnection connection)
        {
            DbCommand? command;
            string query;
            switch (dbType)
            {
                case DatabaseType.MySQL:
                    query = $@"CREATE TABLE IF NOT EXISTS {PlayerStatsTable} (
                                            SteamID VARCHAR(20),
                                            PlayerName VARCHAR(32) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
                                            TimesConnected INT,
                                            LastConnected INT,
                                            GlobalPoints INT,
                                            HideTimerHud BOOL,
                                            HideKeys BOOL,
                                            SoundsEnabled BOOL,
                                            PlayerFov INT,
                                            HudType INT,
                                            IsVip BOOL,
                                            BigGifID VARCHAR(16),
                                            PRIMARY KEY (SteamID)
                                        )";
                    command = new MySqlCommand(query, (MySqlConnection)connection);
                    break;
                case DatabaseType.PostgreSQL:
                    query = $@"CREATE TABLE IF NOT EXISTS ""{PlayerStatsTable}"" (
                                            ""SteamID"" VARCHAR(20) UNIQUE,
                                            ""PlayerName"" VARCHAR(32),
                                            ""TimesConnected"" INT,
                                            ""LastConnected"" INT,
                                            ""GlobalPoints"" INT,
                                            ""HideTimerHud"" BOOL,
                                            ""HideKeys"" BOOL,
                                            ""SoundsEnabled"" BOOL,
                                            ""PlayerFov"" INT,
                                            ""IsVip"" BOOL,
                                            ""BigGifID"" VARCHAR(16),
                                            PRIMARY KEY (""SteamID"")
                                        )";
                    command = new NpgsqlCommand(query, (NpgsqlConnection)connection);
                    break;
                default:
                    command = null;
                    break;
            }
            using (command)
            {
                try
                {
                    await command!.ExecuteNonQueryAsync();
                }
                catch (Exception ex)
                {
                    SharpTimerError($"Error in CreatePlayerStatsTableAsync: {ex.Message}");
                }
            }
        }

        private async Task CreatePlayerReplaysTableAsync(IDbConnection connection)
        {
            DbCommand command;
            string query;

            switch (dbType)
            {
                case DatabaseType.MySQL:
                    query = $@"
                        CREATE TABLE IF NOT EXISTS `PlayerReplays` (
                            `SteamID` VARCHAR(20),
                            `MapName` VARCHAR(255),
                            `Style` INT,
                            `ReplayData` LONGBLOB,
                            PRIMARY KEY (`SteamID`, `MapName`, `Style`)
                        )";
                    command = new MySqlCommand(query, (MySqlConnection)connection);
                    break;
                case DatabaseType.PostgreSQL:
                    query = $@"
                        CREATE TABLE IF NOT EXISTS ""PlayerReplays"" (
                            ""SteamID"" VARCHAR(20),
                            ""MapName"" VARCHAR(255),
                            ""Style"" INT,
                            ""ReplayData"" BYTEA,
                            PRIMARY KEY (""SteamID"", ""MapName"", ""Style"")
                        )";
                    command = new NpgsqlCommand(query, (NpgsqlConnection)connection);
                    break;
                default:
                    throw new Exception("Unsupported database type");
            }

            try
            {
                await command.ExecuteNonQueryAsync();
            }
            catch (Exception ex)
            {
                SharpTimerError($"Error executing table creation: {ex.Message}");
                throw;
            }
        }

        public async Task SavePlayerTimeToDatabase(CCSPlayerController? player, int timerTicks, string steamId, string playerName, int playerSlot, int bonusX = 0, int style = 0)
        {
            SharpTimerDebug($"Trying to save player {(bonusX != 0 ? $"bonus {bonusX} time" : "time")} to database for {playerName} {timerTicks}");
            int currentStyle = playerTimers[playerSlot].currentStyle;
            try
            {
                if (!IsAllowedPlayer(player)) return;
                //if ((bonusX == 0 && !playerTimers[playerSlot].IsTimerRunning) || (bonusX != 0 && !playerTimers[playerSlot].IsBonusTimerRunning)) return;
                string currentMapNamee = bonusX == 0 ? currentMapName! : $"{currentMapName}_bonus{bonusX}";

                int timeNowUnix = (int)DateTimeOffset.UtcNow.ToUnixTimeSeconds();
                // get player columns
                int dBtimesFinished = 0;
                int dBlastFinished = 0;
                int dBunixStamp = 0;
                int dBtimerTicks = 0;
                string dBFormattedTime;

                // store new value separatley
                int new_dBtimerTicks = 0;
                int playerPoints = 0;
                bool beatPB = false;

                using (var connection = await OpenConnectionAsync())
                {
                    await CreatePlayerRecordsTableAsync(connection);

                    string formattedTime = FormatTime(timerTicks);
                    string? selectQuery;
                    DbCommand? selectCommand;
                    switch (dbType)
                    {
                        case DatabaseType.MySQL:
                            selectQuery = @"SELECT TimesFinished, LastFinished, FormattedTime, TimerTicks, UnixStamp FROM PlayerRecords WHERE MapName = @MapName AND SteamID = @SteamID AND Style = @Style";
                            selectCommand = new MySqlCommand(selectQuery, (MySqlConnection)connection);
                            break;
                        case DatabaseType.PostgreSQL:
                            selectQuery = @"SELECT ""TimesFinished"", ""LastFinished"", ""FormattedTime"", ""TimerTicks"", ""UnixStamp"" FROM ""PlayerRecords"" WHERE ""MapName"" = @MapName AND ""SteamID"" = @SteamID AND ""Style"" = @Style";
                            selectCommand = new NpgsqlCommand(selectQuery, (NpgsqlConnection)connection);
                            break;
                        default:
                            selectQuery = null;
                            selectCommand = null;
                            break;
                    }
                    // Check if the record already exists or has a higher timer value
                    selectCommand!.AddParameterWithValue("@MapName", currentMapNamee);
                    selectCommand!.AddParameterWithValue("@SteamID", steamId);
                    selectCommand!.AddParameterWithValue("@Style", style);

                    var row = await selectCommand!.ExecuteReaderAsync();

                    if (row.Read())
                    {
                        // get player columns
                        dBtimesFinished = row.GetInt32("TimesFinished");
                        dBlastFinished = row.GetInt32("LastFinished");
                        dBunixStamp = row.GetInt32("UnixStamp");
                        dBtimerTicks = row.GetInt32("TimerTicks");
                        dBFormattedTime = row.GetString("FormattedTime");

                        // Modify the stats
                        dBtimesFinished++;
                        dBlastFinished = timeNowUnix;
                        if (timerTicks < dBtimerTicks)
                        {
                            new_dBtimerTicks = timerTicks;
                            dBunixStamp = timeNowUnix;
                            dBFormattedTime = formattedTime;
                            playerPoints = timerTicks;
                            beatPB = true;
                            if (enableReplays && !onlySRReplay)
                            {
                                if (useBinaryReplays)
                                {
                                    _ = Task.Run(async () =>
                                        await DumpReplayToBinary(player!, steamId, playerSlot, bonusX, currentStyle));
                                }
                                else if (useDatabaseReplays)
                                {
                                    _ = Task.Run(async () =>
                                        await DumpReplayToDatabase(player!, steamId, playerSlot, bonusX, currentStyle));
                                }
                                else
                                {
                                    _ = Task.Run(async () =>
                                        await DumpReplayToJson(player!, steamId, playerSlot, bonusX, currentStyle));
                                }
                            }
                        }
                        else
                        {
                            new_dBtimerTicks = dBtimerTicks;
                            beatPB = false;
                        }

                        await row.CloseAsync();
                        // Update or insert the record
                        string? upsertQuery;
                        DbCommand? upsertCommand;
                        switch (dbType)
                        {
                            case DatabaseType.MySQL:
                                upsertQuery = @"
                                                    INSERT INTO PlayerRecords 
                                                    (MapName, SteamID, PlayerName, TimerTicks, LastFinished, TimesFinished, FormattedTime, UnixStamp, Style)
                                                    VALUES 
                                                    (@MapName, @SteamID, @PlayerName, @TimerTicks, @LastFinished, @TimesFinished, @FormattedTime, @UnixStamp, @Style)
                                                    ON DUPLICATE KEY UPDATE
                                                    MapName = VALUES(MapName),
                                                    PlayerName = VALUES(PlayerName),
                                                    TimerTicks = VALUES(TimerTicks),
                                                    LastFinished = VALUES(LastFinished),
                                                    TimesFinished = VALUES(TimesFinished),
                                                    FormattedTime = VALUES(FormattedTime),
                                                    UnixStamp = VALUES(UnixStamp),
                                                    Style = VALUES(Style);
                                                    ";
                                upsertCommand = new MySqlCommand(upsertQuery, (MySqlConnection)connection);
                                break;
                            case DatabaseType.PostgreSQL:
                                upsertQuery = @"
                                                    INSERT INTO ""PlayerRecords"" 
                                                    (""MapName"", ""SteamID"", ""PlayerName"", ""TimerTicks"", ""LastFinished"", ""TimesFinished"", ""FormattedTime"", ""UnixStamp"", ""Style"")
                                                    VALUES 
                                                    (@MapName, @SteamID, @PlayerName, @TimerTicks, @LastFinished, @TimesFinished, @FormattedTime, @UnixStamp, @Style)
                                                    ON CONFLICT (""MapName"", ""SteamID"", ""Style"")
                                                    DO UPDATE SET
                                                    ""MapName"" = EXCLUDED.""MapName"",
                                                    ""PlayerName"" = EXCLUDED.""PlayerName"",
                                                    ""TimerTicks"" = EXCLUDED.""TimerTicks"",
                                                    ""LastFinished"" = EXCLUDED.""LastFinished"",
                                                    ""TimesFinished"" = EXCLUDED.""TimesFinished"",
                                                    ""FormattedTime"" = EXCLUDED.""FormattedTime"",
                                                    ""UnixStamp"" = EXCLUDED.""UnixStamp"",
                                                    ""Style"" = EXCLUDED.""Style"";
                                                    ";
                                upsertCommand = new NpgsqlCommand(upsertQuery, (NpgsqlConnection)connection);
                                break;
                            default:
                                upsertQuery = null;
                                upsertCommand = null;
                                break;
                        }
                        using (upsertCommand)
                        {
                            upsertCommand!.AddParameterWithValue("@MapName", currentMapNamee);
                            upsertCommand!.AddParameterWithValue("@PlayerName", playerName);
                            upsertCommand!.AddParameterWithValue("@TimesFinished", dBtimesFinished);
                            upsertCommand!.AddParameterWithValue("@LastFinished", dBlastFinished);
                            upsertCommand!.AddParameterWithValue("@TimerTicks", new_dBtimerTicks);
                            upsertCommand!.AddParameterWithValue("@FormattedTime", dBFormattedTime);
                            upsertCommand!.AddParameterWithValue("@UnixStamp", dBunixStamp);
                            upsertCommand!.AddParameterWithValue("@SteamID", steamId);
                            upsertCommand!.AddParameterWithValue("@Style", style);
                            if (style == 0 && (stageTriggerCount != 0 || cpTriggerCount != 0) && bonusX == 0 && enableDb && timerTicks < dBtimerTicks && !ignoreJSON) Server.NextFrame(() => _ = Task.Run(async () => await DumpPlayerStageTimesToJson(player, steamId, playerSlot)));
                            var prevSRID = await GetMapRecordSteamIDFromDatabase(bonusX, 0, style);
                            var prevSR = await GetPreviousPlayerRecordFromDatabase(prevSRID.Item1, currentMapNamee, prevSRID.Item2, bonusX, style);
                            await upsertCommand!.ExecuteNonQueryAsync();
                            Server.NextFrame(() => SharpTimerDebug($"Saved player {(bonusX != 0 ? $"bonus {bonusX} time" : "time")} to database for {playerName} {timerTicks} {DateTimeOffset.UtcNow.ToUnixTimeSeconds()}"));
                            if (enableDb && IsAllowedPlayer(player)) await RankCommandHandler(player, steamId, playerSlot, playerName, true, style);
                            if (globalRanksEnabled == true) await SavePlayerPoints(steamId, playerName, playerSlot, timerTicks, dBtimerTicks, beatPB, bonusX, style, dBtimesFinished);
                            if (IsAllowedPlayer(player)) Server.NextFrame(() => _ = Task.Run(async () => await PrintMapTimeToChat(player!, steamId, playerName, dBtimerTicks, timerTicks, bonusX, dBtimesFinished, style, prevSR)));
                            if (enableReplays && onlySRReplay && (prevSR == 0 || prevSR > timerTicks))
                            {
                                if (useBinaryReplays)
                                {
                                    _ = Task.Run(async () =>
                                        await DumpReplayToBinary(player!, steamId, playerSlot, bonusX, currentStyle));
                                }
                                else if (useDatabaseReplays)
                                {
                                    _ = Task.Run(async () =>
                                        await DumpReplayToDatabase(player!, steamId, playerSlot, bonusX, currentStyle));
                                }
                                else
                                {
                                    _ = Task.Run(async () =>
                                        await DumpReplayToJson(player!, steamId, playerSlot, bonusX, currentStyle));
                                }
                            }

                            /*
                            Server.NextFrame(async () =>
                            {
                                var (hostname, ip) = GetHostnameAndIp();
                                var (globalCheck, maxVel, maxWish) = CheckCvarsAndMaxVelo();
                                if (!globalCheck)
                                    return;
                                if (apiKey == "")
                                    return;

                                //first lets see if the new record beats global pb
                                var beatGlobalPB = false;
                                var prevPBTicks = await GetPreviousPlayerRecordFromGlobal(steamId, currentMapName!, playerName, bonusX, style);
                                if (prevPBTicks > timerTicks || prevPBTicks == 0)
                                    beatGlobalPB = true;

                                var record_payload = new List<Record>
                                {
                                    new Record
                                    {
                                        map_name = currentMapNamee,
                                        workshop_id = currentAddonID,
                                        timer_ticks = timerTicks,
                                        steamid = steamId,
                                        player_name = playerName,
                                        formatted_time = FormatTime(timerTicks),
                                        unix_stamp = DateTimeOffset.UtcNow.ToUnixTimeSeconds(),
                                        times_finished = dBtimesFinished,
                                        style = style,
                                        points = await CalculatePlayerPoints(steamId, playerName, timerTicks, dBtimerTicks, beatPB, bonusX, style, dBtimesFinished, currentMapNamee, true),
                                        max_velocity = (int)maxVel,
                                        air_max_wishspeed = (float)maxWish,
                                        hostname = hostname,
                                        ip = ip,
                                        hash = GetHash()
                                    }
                                };

                                _ = Task.Run(async () =>
                                {
                                    await SubmitRecordAsync(record_payload); // submit the record to db to generate new record_id
                                }).ContinueWith(async task =>                // THEN submit the replay using the record_id
                                {
                                    // only submit replay if beat global pb
                                    if (beatGlobalPB)
                                    {
                                        var replay_payload = new ReplayData
                                        {
                                            record_id = await GetRecordIDAsync(new { map_name = record_payload[0].map_name, unix_stamp = record_payload[0].unix_stamp }),
                                            map_name = currentMapNamee,
                                            style = style,
                                            hash = GetHash(),
                                            replay_data = await GetReplayJson(player!, player!.Slot)
                                        };

                                        await SubmitReplayAsync(replay_payload);
                                    }
                                });
                            });
                            */
                        }

                    }
                    else
                    {
                        Server.NextFrame(() => SharpTimerDebug($"No player record yet"));
                        if (enableReplays && !onlySRReplay)
                        {
                            if (useBinaryReplays)
                            {
                                _ = Task.Run(async () =>
                                    await DumpReplayToBinary(player!, steamId, playerSlot, bonusX, currentStyle));
                            }
                            else if (useDatabaseReplays)
                            {
                                _ = Task.Run(async () =>
                                    await DumpReplayToDatabase(player!, steamId, playerSlot, bonusX, currentStyle));
                            }
                            else
                            {
                                _ = Task.Run(async () =>
                                    await DumpReplayToJson(player!, steamId, playerSlot, bonusX, currentStyle));
                            }
                        }
                        await row.CloseAsync();

                        string? upsertQuery;
                        DbCommand? upsertCommand;
                        switch (dbType)
                        {
                            case DatabaseType.MySQL:
                                upsertQuery = @"REPLACE INTO PlayerRecords (MapName, SteamID, PlayerName, TimerTicks, LastFinished, TimesFinished, FormattedTime, UnixStamp, Style) VALUES (@MapName, @SteamID, @PlayerName, @TimerTicks, @LastFinished, @TimesFinished, @FormattedTime, @UnixStamp, @Style)";
                                upsertCommand = new MySqlCommand(upsertQuery, (MySqlConnection)connection);
                                break;
                            case DatabaseType.PostgreSQL:
                                upsertQuery = @"INSERT INTO ""PlayerRecords"" (""MapName"", ""SteamID"", ""PlayerName"", ""TimerTicks"", ""LastFinished"", ""TimesFinished"", ""FormattedTime"", ""UnixStamp"", ""Style"") VALUES (@MapName, @SteamID, @PlayerName, @TimerTicks, @LastFinished, @TimesFinished, @FormattedTime, @UnixStamp, @Style)";
                                upsertCommand = new NpgsqlCommand(upsertQuery, (NpgsqlConnection)connection);
                                break;
                            default:
                                upsertQuery = null;
                                upsertCommand = null;
                                break;
                        }

                        using (upsertCommand)
                        {
                            upsertCommand!.AddParameterWithValue("@MapName", currentMapNamee);
                            upsertCommand!.AddParameterWithValue("@PlayerName", playerName);
                            upsertCommand!.AddParameterWithValue("@TimesFinished", 1);
                            upsertCommand!.AddParameterWithValue("@LastFinished", timeNowUnix);
                            upsertCommand!.AddParameterWithValue("@TimerTicks", timerTicks);
                            upsertCommand!.AddParameterWithValue("@FormattedTime", formattedTime);
                            upsertCommand!.AddParameterWithValue("@UnixStamp", timeNowUnix);
                            upsertCommand!.AddParameterWithValue("@SteamID", steamId);
                            upsertCommand!.AddParameterWithValue("@Style", style);
                            var prevSRID = await GetMapRecordSteamIDFromDatabase(bonusX, 0, style);
                            var prevSR = await GetPreviousPlayerRecordFromDatabase(prevSRID.Item1, currentMapNamee, prevSRID.Item2, bonusX, style);
                            await upsertCommand!.ExecuteNonQueryAsync();
                            if (globalRanksEnabled == true) await SavePlayerPoints(steamId, playerName, playerSlot, timerTicks, dBtimerTicks, beatPB, bonusX, style, dBtimesFinished);
                            if (style == 0 && (stageTriggerCount != 0 || cpTriggerCount != 0) && bonusX == 0 && !ignoreJSON) Server.NextFrame(() => _ = Task.Run(async () => await DumpPlayerStageTimesToJson(player, steamId, playerSlot)));
                            Server.NextFrame(() => SharpTimerDebug($"Saved player {(bonusX != 0 ? $"bonus {bonusX} time" : "time")} to database for {playerName} {timerTicks} {DateTimeOffset.UtcNow.ToUnixTimeSeconds()}"));
                            if (IsAllowedPlayer(player)) await RankCommandHandler(player, steamId, playerSlot, playerName, true, style);
                            if (IsAllowedPlayer(player)) Server.NextFrame(() => _ = Task.Run(async () => await PrintMapTimeToChat(player!, steamId, playerName, dBtimerTicks, timerTicks, bonusX, 1, style, prevSR)));
                            if (enableReplays && onlySRReplay && (prevSR == 0 || prevSR > timerTicks))
                            {
                                if (useBinaryReplays)
                                {
                                    _ = Task.Run(async () =>
                                        await DumpReplayToBinary(player!, steamId, playerSlot, bonusX, currentStyle));
                                }
                                else if (useDatabaseReplays)
                                {
                                    _ = Task.Run(async () =>
                                        await DumpReplayToDatabase(player!, steamId, playerSlot, bonusX, currentStyle));
                                }
                                else
                                {
                                    _ = Task.Run(async () =>
                                        await DumpReplayToJson(player!, steamId, playerSlot, bonusX, currentStyle));
                                }
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Server.NextFrame(() => SharpTimerError($"Error saving player {(bonusX != 0 ? $"bonus {bonusX} time" : "time")} to database: {ex.Message}"));
            }
        }

        public async Task GetPlayerStats(CCSPlayerController? player, string steamId, string playerName, int playerSlot, bool fromConnect)
        {
            SharpTimerDebug($"Trying to get player stats from database for {playerName}");
            try
            {
                if (player == null || !player.IsValid || player.IsBot) return;
                if (!(connectedPlayers.ContainsKey(playerSlot) && playerTimers.ContainsKey(playerSlot))) return;

                int timeNowUnix = (int)DateTimeOffset.UtcNow.ToUnixTimeSeconds();
                // get player columns
                int timesConnected = 0;
                int lastConnected = 0;
                bool hideTimerHud = false;
                bool hideKeys = false;
                bool soundsEnabled = true;
                int playerFov = 0;
                int hudType = 1;
                bool isVip = false;
                string bigGif = "x";
                int playerPoints = 0;
                bool hideWeapon = false;
                bool hidePlayers = false;

                using (var connection = await OpenConnectionAsync())
                {
                    await CreatePlayerStatsTableAsync(connection);

                    string? selectQuery;
                    DbCommand? selectCommand;
                    switch (dbType)
                    {
                        case DatabaseType.MySQL:
                            selectQuery = $@"SELECT PlayerName, TimesConnected, LastConnected, HideTimerHud, HideKeys, SoundsEnabled, PlayerFov, HudType, IsVip, BigGifID, GlobalPoints, HideWeapon, HidePlayers FROM {PlayerStatsTable} WHERE SteamID = @SteamID";
                            selectCommand = new MySqlCommand(selectQuery, (MySqlConnection)connection);
                            break;
                        case DatabaseType.PostgreSQL:
                            selectQuery = $@"SELECT ""PlayerName"", ""TimesConnected"", ""LastConnected"", ""HideTimerHud"", ""HideKeys"", ""SoundsEnabled"", ""PlayerFov"", ""IsVip"", ""BigGifID"", ""GlobalPoints"", ""HideWeapon"", ""HidePlayers"" FROM ""{PlayerStatsTable}"" WHERE ""SteamID"" = @SteamID";
                            selectCommand = new NpgsqlCommand(selectQuery, (NpgsqlConnection)connection);
                            break;
                        default:
                            selectQuery = null;
                            selectCommand = null;
                            break;
                    }

                    using (selectCommand)
                    {
                        selectCommand!.AddParameterWithValue("@SteamID", steamId);

                        var row = await selectCommand!.ExecuteReaderAsync();

                        if (row.Read())
                        {
                            // get player columns
                            switch (dbType)
                            {
                                case DatabaseType.MySQL:
                                case DatabaseType.PostgreSQL:
                                    timesConnected = row.GetInt32("TimesConnected");
                                    hideTimerHud = row.GetBoolean("HideTimerHud");
                                    hideKeys = row.GetBoolean("HideKeys");
                                    hideWeapon = row.GetBoolean("HideWeapon");
                                    hidePlayers = row.GetBoolean("HidePlayers");
                                    soundsEnabled = row.GetBoolean("SoundsEnabled");
                                    playerFov = row.GetInt32("PlayerFov");
                                    hudType = row.GetInt32("HudType");
                                    isVip = row.GetBoolean("IsVip");
                                    bigGif = row.GetString("BigGifID");
                                    playerPoints = row.GetInt32("GlobalPoints");
                                    break;
                            }

                            // Modify the stats
                            timesConnected++;
                            lastConnected = timeNowUnix;
                            Server.NextFrame(() =>
                            {
                                if (playerTimers.TryGetValue(playerSlot, out PlayerTimerInfo? value))
                                {
                                    value.HideTimerHud = hideTimerHud;
                                    value.HideKeys = hideKeys;
                                    value.HideWeapon = hideWeapon;
                                    value.HidePlayers = hidePlayers;
                                    value.SoundsEnabled = soundsEnabled;
                                    value.PlayerFov = playerFov;
                                    value.CurrentHudType = (PlayerTimerInfo.HudType)hudType;
                                    value.IsVip = isVip;
                                    value.VipBigGif = bigGif;
                                    value.TimesConnected = timesConnected;
                                }
                                else
                                {
                                    SharpTimerError($"Error getting player stats from database for {playerName}: player was not on the server anymore");
                                    return;
                                }
                            });

                            await row.CloseAsync();
                            // Update or insert the record

                            string? upsertQuery;
                            DbCommand? upsertCommand;
                            switch (dbType)
                            {
                                case DatabaseType.MySQL:
                                    upsertQuery = $@"
                                    INSERT INTO {PlayerStatsTable}
                                        (PlayerName, SteamID, TimesConnected, LastConnected, HideTimerHud, HideKeys, SoundsEnabled, PlayerFov, HudType, IsVip, BigGifID, GlobalPoints, HideWeapon, HidePlayers)
                                    VALUES
                                        (@PlayerName, @SteamID, @TimesConnected, @LastConnected, @HideTimerHud, @HideKeys, @SoundsEnabled, @PlayerFov, @HudType, @IsVip, @BigGifID, @GlobalPoints, @HideWeapon, @HidePlayers)
                                    ON DUPLICATE KEY UPDATE
                                        PlayerName     = @PlayerName,
                                        TimesConnected = @TimesConnected,
                                        LastConnected  = @LastConnected,
                                        HideTimerHud   = @HideTimerHud,
                                        HideKeys       = @HideKeys,
                                        SoundsEnabled  = @SoundsEnabled,
                                        PlayerFov      = @PlayerFov,
                                        HudType        = @HudType,
                                        IsVip          = @IsVip,
                                        BigGifID       = @BigGifID,
                                        GlobalPoints   = @GlobalPoints,
                                        HideWeapon     = @HideWeapon,
                                        HidePlayers    = @HidePlayers;";
                                    upsertCommand = new MySqlCommand(upsertQuery, (MySqlConnection)connection);
                                    break;

                                case DatabaseType.PostgreSQL:
                                    upsertQuery = $@"
                                    INSERT INTO ""{PlayerStatsTable}""
                                        (""PlayerName"", ""SteamID"", ""TimesConnected"", ""LastConnected"", ""HideTimerHud"", ""HideKeys"", ""SoundsEnabled"", ""PlayerFov"", ""IsVip"", ""BigGifID"", ""GlobalPoints"", ""HideWeapon"", ""HidePlayers"")
                                    VALUES
                                        (@PlayerName, @SteamID, @TimesConnected, @LastConnected, @HideTimerHud, @HideKeys, @SoundsEnabled, @PlayerFov, @IsVip, @BigGifID, @GlobalPoints, @HideWeapon, @HidePlayers)
                                    ON CONFLICT (""SteamID"")
                                    DO UPDATE SET
                                        ""PlayerName""     = EXCLUDED.""PlayerName"",
                                        ""TimesConnected"" = EXCLUDED.""TimesConnected"",
                                        ""LastConnected""  = EXCLUDED.""LastConnected"",
                                        ""HideTimerHud""   = EXCLUDED.""HideTimerHud"",
                                        ""HideKeys""       = EXCLUDED.""HideKeys"",
                                        ""SoundsEnabled""  = EXCLUDED.""SoundsEnabled"",
                                        ""PlayerFov""      = EXCLUDED.""PlayerFov"",
                                        ""IsVip""          = EXCLUDED.""IsVip"",
                                        ""BigGifID""       = EXCLUDED.""BigGifID"",
                                        ""GlobalPoints""   = EXCLUDED.""GlobalPoints"",
                                        ""HideWeapon""     = EXCLUDED.""HideWeapon"",
                                        ""HidePlayers""    = EXCLUDED.""HidePlayers"";";
                                    upsertCommand = new NpgsqlCommand(upsertQuery, (NpgsqlConnection)connection);
                                    break;

                                default:
                                    upsertQuery = null;
                                    upsertCommand = null;
                                    break;
                            }

                            using (upsertCommand)
                            {
                                upsertCommand!.AddParameterWithValue("@PlayerName", playerName);
                                upsertCommand!.AddParameterWithValue("@SteamID", steamId);
                                upsertCommand!.AddParameterWithValue("@TimesConnected", timesConnected);
                                upsertCommand!.AddParameterWithValue("@LastConnected", lastConnected);
                                upsertCommand!.AddParameterWithValue("@HideTimerHud", hideTimerHud);
                                upsertCommand!.AddParameterWithValue("@HideKeys", hideKeys);
                                upsertCommand!.AddParameterWithValue("@SoundsEnabled", soundsEnabled);
                                upsertCommand!.AddParameterWithValue("@PlayerFov", playerFov);
                                upsertCommand!.AddParameterWithValue("@HudType", hudType);
                                upsertCommand!.AddParameterWithValue("@IsVip", isVip);
                                upsertCommand!.AddParameterWithValue("@BigGifID", bigGif);
                                upsertCommand!.AddParameterWithValue("@GlobalPoints", playerPoints);
                                upsertCommand!.AddParameterWithValue("@HideWeapon", hideWeapon);
                                upsertCommand!.AddParameterWithValue("@HidePlayers", hidePlayers);

                                await upsertCommand!.ExecuteNonQueryAsync();
                                Server.NextFrame(() => SharpTimerDebug($"Got player stats from database for {playerName}"));
                                if (connectMsgEnabled) Server.NextFrame(() => Server.PrintToChatAll($"{Localizer["prefix"]} {Localizer["connected_message", playerName, FormatOrdinal(timesConnected)]}"));
                            }

                        }
                        else
                        {
                            Server.NextFrame(() => SharpTimerDebug($"No player stats yet"));
                            await row.CloseAsync();

                            string? upsertQuery;
                            DbCommand? upsertCommand;
                            switch (dbType)
                            {
                                case DatabaseType.MySQL:
                                    upsertQuery = $@"REPLACE INTO {PlayerStatsTable} (PlayerName, SteamID, TimesConnected, LastConnected, HideTimerHud, HideKeys, SoundsEnabled, PlayerFov, HudType, IsVip, BigGifID, GlobalPoints, HideWeapon, HidePlayers) VALUES (@PlayerName, @SteamID, @TimesConnected, @LastConnected, @HideTimerHud, @HideKeys, @SoundsEnabled, @PlayerFov, @HudType, @IsVip, @BigGifID, @GlobalPoints, @HideWeapon, @HidePlayers)";
                                    upsertCommand = new MySqlCommand(upsertQuery, (MySqlConnection)connection);
                                    break;
                                case DatabaseType.PostgreSQL:
                                    upsertQuery = $@"INSERT INTO ""{PlayerStatsTable}"" (""PlayerName"", ""SteamID"", ""TimesConnected"", ""LastConnected"", ""HideTimerHud"", ""HideKeys"", ""SoundsEnabled"", ""PlayerFov"", ""IsVip"", ""BigGifID"", ""GlobalPoints"", ""HideWeapon"", ""HidePlayers"") VALUES (@PlayerName, @SteamID, @TimesConnected, @LastConnected, @HideTimerHud, @HideKeys, @SoundsEnabled, @PlayerFov, @IsVip, @BigGifID, @GlobalPoints, @HideWeapon, @HidePlayers)";
                                    upsertCommand = new NpgsqlCommand(upsertQuery, (NpgsqlConnection)connection);
                                    break;
                                default:
                                    upsertQuery = null;
                                    upsertCommand = null;
                                    break;
                            }

                            using (upsertCommand)
                            {
                                upsertCommand!.AddParameterWithValue("@PlayerName", playerName);
                                upsertCommand!.AddParameterWithValue("@SteamID", steamId);
                                upsertCommand!.AddParameterWithValue("@TimesConnected", 1);
                                upsertCommand!.AddParameterWithValue("@LastConnected", timeNowUnix);
                                upsertCommand!.AddParameterWithValue("@HideTimerHud", false);
                                upsertCommand!.AddParameterWithValue("@HideKeys", false);
                                upsertCommand!.AddParameterWithValue("@SoundsEnabled", soundsEnabledByDefault);
                                upsertCommand!.AddParameterWithValue("@PlayerFov", 0);
                                upsertCommand!.AddParameterWithValue("@HudType", 1);
                                upsertCommand!.AddParameterWithValue("@IsVip", false);
                                upsertCommand!.AddParameterWithValue("@BigGifID", "x");
                                upsertCommand!.AddParameterWithValue("@GlobalPoints", 0);
                                upsertCommand!.AddParameterWithValue("@HideWeapon", false);
                                upsertCommand!.AddParameterWithValue("@HidePlayers", false);

                                await upsertCommand!.ExecuteNonQueryAsync();
                                Server.NextFrame(() => SharpTimerDebug($"Got player stats from database for {playerName}"));
                                if (connectMsgEnabled) Server.NextFrame(() => Server.PrintToChatAll($"{Localizer["prefix"]} {Localizer["connected_message_first", playerName]}"));
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
        public async Task SavePlayerStageTimeToDatabase(CCSPlayerController? player, int timerTicks, int stage, string velocity, string steamId, string playerName, int playerSlot, int bonusX = 0, int style = 0)
        {
            SharpTimerDebug($"Trying to save player {(bonusX != 0 ? $"bonus {bonusX} stage {stage} time" : $"stage {stage} time")} to database for {playerName} {timerTicks}");
            try
            {
                if (!IsAllowedPlayer(player)) return;
                //if ((bonusX == 0 && !playerTimers[playerSlot].IsTimerRunning) || (bonusX != 0 && !playerTimers[playerSlot].IsBonusTimerRunning)) return;
                string currentMapNamee = bonusX == 0 ? currentMapName! : $"{currentMapName}_bonus{bonusX}";

                int timeNowUnix = (int)DateTimeOffset.UtcNow.ToUnixTimeSeconds();
                // get player columns
                int dBtimerTicks = 0;
                string dBFormattedTime;

                // store new value separatley
                int new_dBtimerTicks = 0;
                int playerPoints = 0;

                using (var connection = await OpenConnectionAsync())
                {
                    string formattedTime = FormatTime(timerTicks);
                    string? selectQuery;
                    DbCommand? selectCommand;
                    switch (dbType)
                    {
                        case DatabaseType.MySQL:
                            selectQuery = @"SELECT FormattedTime, TimerTicks FROM PlayerStageTimes WHERE MapName = @MapName AND SteamID = @SteamID AND Stage = @Stage";
                            selectCommand = new MySqlCommand(selectQuery, (MySqlConnection)connection);
                            break;
                        case DatabaseType.PostgreSQL:
                            selectQuery = @"SELECT ""FormattedTime"", ""TimerTicks"" FROM ""PlayerStageTimes"" WHERE ""MapName"" = @MapName AND ""SteamID"" = @SteamID AND ""Stage"" = @Stage";
                            selectCommand = new NpgsqlCommand(selectQuery, (NpgsqlConnection)connection);
                            break;
                        default:
                            selectQuery = null;
                            selectCommand = null;
                            break;
                    }
                    // Check if the record already exists or has a higher timer value
                    selectCommand!.AddParameterWithValue("@MapName", currentMapNamee);
                    selectCommand!.AddParameterWithValue("@SteamID", steamId);
                    selectCommand!.AddParameterWithValue("@Stage", stage);

                    var row = await selectCommand!.ExecuteReaderAsync();

                    if (row.Read())
                    {
                        // get player columns
                        dBtimerTicks = row.GetInt32("TimerTicks");
                        dBFormattedTime = row.GetString("FormattedTime");

                        // Modify the stats
                        if (timerTicks < dBtimerTicks)
                        {
                            new_dBtimerTicks = timerTicks;
                            dBFormattedTime = formattedTime;
                            playerPoints = timerTicks;
                            if (playerPoints < 32)
                            {
                                playerPoints = 320000;
                            }
                            //not saving replays for stage times
                            //if (enableReplays == true && enableDb) _ = Task.Run(async () => await DumpReplayToJson(player!, steamId, playerSlot, bonusX, playerTimers[playerSlot].currentStyle));
                        }
                        else
                        {
                            new_dBtimerTicks = dBtimerTicks;
                            playerPoints = 320000;
                        }

                        await row.CloseAsync();
                        // Update or insert the record
                        string? upsertQuery;
                        DbCommand? upsertCommand;
                        switch (dbType)
                        {
                            case DatabaseType.MySQL:
                                upsertQuery = @"
                                                    INSERT INTO PlayerStageTimes 
                                                    (MapName, SteamID, PlayerName, Stage, TimerTicks, FormattedTime, Velocity)
                                                    VALUES 
                                                    (@MapName, @SteamID, @PlayerName, @Stage, @TimerTicks, @FormattedTime, @Velocity)
                                                    ON DUPLICATE KEY UPDATE
                                                    MapName = VALUES(MapName),
                                                    PlayerName = VALUES(PlayerName),
                                                    Stage = VALUES(Stage),
                                                    TimerTicks = VALUES(TimerTicks),
                                                    FormattedTime = VALUES(FormattedTime),
                                                    Velocity = VALUES(Velocity);
                                                    ";
                                upsertCommand = new MySqlCommand(upsertQuery, (MySqlConnection)connection);
                                break;
                            case DatabaseType.PostgreSQL:
                                upsertQuery = @"
                                                    INSERT INTO ""PlayerStageTimes"" 
                                                    (""MapName"", ""SteamID"", ""PlayerName"", ""Stage"", ""TimerTicks"", ""FormattedTime"", ""Velocity"")
                                                    VALUES 
                                                    (@MapName, @SteamID, @PlayerName, @Stage, @TimerTicks, @FormattedTime, @Velocity)
                                                    ON CONFLICT (""MapName"", ""SteamID"", ""Stage"")
                                                    DO UPDATE SET
                                                    ""MapName"" = EXCLUDED.""MapName"",
                                                    ""PlayerName"" = EXCLUDED.""PlayerName"",
                                                    ""Stage"" = EXCLUDED.""Stage"",
                                                    ""TimerTicks"" = EXCLUDED.""TimerTicks"",
                                                    ""FormattedTime"" = EXCLUDED.""FormattedTime"",
                                                    ""Velocity"" = EXCLUDED.""Velocity"";
                                                    ";
                                upsertCommand = new NpgsqlCommand(upsertQuery, (NpgsqlConnection)connection);
                                break;
                            default:
                                upsertQuery = null;
                                upsertCommand = null;
                                break;
                        }
                        using (upsertCommand)
                        {
                            upsertCommand!.AddParameterWithValue("@MapName", currentMapNamee);
                            upsertCommand!.AddParameterWithValue("@PlayerName", playerName);
                            upsertCommand!.AddParameterWithValue("@TimerTicks", new_dBtimerTicks);
                            upsertCommand!.AddParameterWithValue("@FormattedTime", dBFormattedTime);
                            upsertCommand!.AddParameterWithValue("@SteamID", steamId);
                            upsertCommand!.AddParameterWithValue("@Stage", stage);
                            upsertCommand!.AddParameterWithValue("@Velocity", velocity);
                            //no points for stage times until points overhaul
                            //if (enableDb && globalRanksEnabled == true && ((dBtimesFinished <= maxGlobalFreePoints && globalRanksFreePointsEnabled == true) || beatPB)) await SavePlayerPoints(steamId, playerName, playerSlot, playerPoints, dBtimerTicks, beatPB, bonusX, style);
                            //dont save stagetimes unless they complete map
                            //if ((stageTriggerCount != 0 || cpTriggerCount != 0) && bonusX == 0 && enableDb && timerTicks < dBtimerTicks) Server.NextFrame(() => _ = Task.Run(async () => await DumpPlayerStageTimesToJson(player, steamId, playerSlot)));
                            var prevSRID = await GetStageRecordSteamIDFromDatabase(bonusX, 0, style);
                            var prevSR = await GetPreviousPlayerStageRecordFromDatabase(player, prevSRID.Item1, currentMapNamee, stage, prevSRID.Item2, bonusX);
                            await upsertCommand!.ExecuteNonQueryAsync();
                            Server.NextFrame(() => SharpTimerDebug($"Saved player {(bonusX != 0 ? $"bonus {bonusX} stage {stage} time" : $"{stage} time")} to database for {playerName} {timerTicks} {DateTimeOffset.UtcNow.ToUnixTimeSeconds()}"));
                            if (IsAllowedPlayer(player)) Server.NextFrame(() => _ = Task.Run(async () => await PrintStageTimeToChat(player!, steamId, playerName, dBtimerTicks, timerTicks, stage, bonusX, prevSR)));
                        }
                    }
                    else
                    {
                        Server.NextFrame(() => SharpTimerDebug($"No player record yet"));
                        //dont save stagetimes unless they complete map
                        //if (enableReplays == true && usePostgres == true) _ = Task.Run(async () => await DumpReplayToJson(player!, steamId, playerSlot, bonusX, playerTimers[playerSlot].currentStyle));
                        await row.CloseAsync();

                        string? upsertQuery;
                        DbCommand? upsertCommand;
                        switch (dbType)
                        {
                            case DatabaseType.MySQL:
                                upsertQuery = @"REPLACE INTO PlayerStageTimes (MapName, SteamID, PlayerName, Stage, TimerTicks, FormattedTime, Velocity) VALUES (@MapName, @SteamID, @PlayerName, @Stage, @TimerTicks, @FormattedTime, @Velocity)";
                                upsertCommand = new MySqlCommand(upsertQuery, (MySqlConnection)connection);
                                break;
                            case DatabaseType.PostgreSQL:
                                upsertQuery = @"INSERT INTO ""PlayerStageTimes"" (""MapName"", ""SteamID"", ""PlayerName"", ""Stage"", ""TimerTicks"", ""FormattedTime"", ""Velocity"") VALUES (@MapName, @SteamID, @PlayerName, @Stage, @TimerTicks, @FormattedTime, @Velocity)";
                                upsertCommand = new NpgsqlCommand(upsertQuery, (NpgsqlConnection)connection);
                                break;
                            default:
                                upsertQuery = null;
                                upsertCommand = null;
                                break;
                        }

                        using (upsertCommand)
                        {
                            upsertCommand!.AddParameterWithValue("@MapName", currentMapNamee);
                            upsertCommand!.AddParameterWithValue("@PlayerName", playerName);
                            upsertCommand!.AddParameterWithValue("@TimerTicks", timerTicks);
                            upsertCommand!.AddParameterWithValue("@FormattedTime", formattedTime);
                            upsertCommand!.AddParameterWithValue("@SteamID", steamId);
                            upsertCommand!.AddParameterWithValue("@Stage", stage);
                            upsertCommand!.AddParameterWithValue("@Velocity", velocity);
                            var prevSRID = await GetStageRecordSteamIDFromDatabase(bonusX, 0, style);
                            var prevSR = await GetPreviousPlayerStageRecordFromDatabase(player, prevSRID.Item1, currentMapNamee, stage, prevSRID.Item2, bonusX);
                            await upsertCommand!.ExecuteNonQueryAsync();
                            //no points until points overhaul
                            //if (globalRanksEnabled == true) await SavePlayerPoints(steamId, playerName, playerSlot, timerTicks, dBtimerTicks, beatPB, bonusX, style);
                            //dont save stagetimes unless they complete map
                            //if ((stageTriggerCount != 0 || cpTriggerCount != 0) && bonusX == 0) Server.NextFrame(() => _ = Task.Run(async () => await DumpPlayerStageTimesToJson(player, steamId, playerSlot)));
                            Server.NextFrame(() => SharpTimerDebug($"Saved player {(bonusX != 0 ? $"bonus {bonusX} stage {stage} time" : $"stage {stage} time")} to database for {playerName} {timerTicks} {DateTimeOffset.UtcNow.ToUnixTimeSeconds()}"));
                            if (IsAllowedPlayer(player) && enableStageTimes) Server.NextFrame(() => _ = Task.Run(async () => await PrintStageTimeToChat(player!, steamId, playerName, dBtimerTicks, timerTicks, stage, bonusX, prevSR)));
                        }

                    }
                }
            }
            catch (Exception ex)
            {
                Server.NextFrame(() => SharpTimerError($"Error saving player {(bonusX != 0 ? $"bonus {bonusX} time" : "time")} to database: {ex.Message}"));
            }
        }

        public async Task SetPlayerStats(CCSPlayerController? player, string steamId, string playerName, int playerSlot)
        {
            SharpTimerDebug($"Trying to set player stats in database for {playerName}");
            try
            {
                if (!IsAllowedPlayer(player)) return;
                int timeNowUnix = (int)DateTimeOffset.UtcNow.ToUnixTimeSeconds();
                int timesConnected = 0;
                int lastConnected = 0;
                bool isVip = false;
                string bigGif = "x";
                int playerPoints = 0;

                using (var connection = await OpenConnectionAsync())
                {
                    await CreatePlayerStatsTableAsync(connection);
                    string? selectQuery;
                    DbCommand? selectCommand;
                    switch (dbType)
                    {
                        case DatabaseType.MySQL:
                            selectQuery = $"SELECT PlayerName, TimesConnected, IsVip, BigGifID, GlobalPoints FROM {PlayerStatsTable} WHERE SteamID = @SteamID";
                            selectCommand = new MySqlCommand(selectQuery, (MySqlConnection)connection);
                            break;
                        case DatabaseType.PostgreSQL:
                            selectQuery = $@"SELECT ""PlayerName"", ""TimesConnected"", ""IsVip"", ""BigGifID"", ""GlobalPoints"" FROM ""{PlayerStatsTable}"" WHERE ""SteamID"" = @SteamID";
                            selectCommand = new NpgsqlCommand(selectQuery, (NpgsqlConnection)connection);
                            break;
                        default:
                            selectQuery = null;
                            selectCommand = null;
                            break;
                    }

                    using (selectCommand)
                    {
                        selectCommand!.AddParameterWithValue("@SteamID", steamId);

                        var row = await selectCommand!.ExecuteReaderAsync();

                        if (row.Read())
                        {
                            // get player columns
                            switch (dbType)
                            {
                                case DatabaseType.MySQL:
                                case DatabaseType.PostgreSQL:
                                    timesConnected = row.GetInt32("TimesConnected");
                                    isVip = row.GetBoolean("IsVip");
                                    bigGif = row.GetString("BigGifID");
                                    playerPoints = row.GetInt32("GlobalPoints");
                                    break;
                            }

                            await row.CloseAsync();
                            // Update or insert the record

                            // upsert (existing-row path)
                            string? upsertQuery;
                            DbCommand? upsertCommand;
                            switch (dbType)
                            {
                                // this one
                                case DatabaseType.MySQL:
                                upsertQuery = $@"
                                INSERT INTO {PlayerStatsTable}
                                    (PlayerName, SteamID, TimesConnected, LastConnected, HideTimerHud, HideKeys, SoundsEnabled, PlayerFov, HudType, IsVip, BigGifID, GlobalPoints, HideWeapon, HidePlayers)
                                VALUES
                                    (@PlayerName, @SteamID, @TimesConnected, @LastConnected, @HideTimerHud, @HideKeys, @SoundsEnabled, @PlayerFov, @HudType, @IsVip, @BigGifID, @GlobalPoints, @HideWeapon, @HidePlayers)
                                ON DUPLICATE KEY UPDATE
                                    PlayerName     = @PlayerName,
                                    TimesConnected = @TimesConnected,
                                    LastConnected  = @LastConnected,
                                    HideTimerHud   = @HideTimerHud,
                                    HideKeys       = @HideKeys,
                                    SoundsEnabled  = @SoundsEnabled,
                                    PlayerFov      = @PlayerFov,
                                    HudType        = @HudType,
                                    IsVip          = @IsVip,
                                    BigGifID       = @BigGifID,
                                    HideWeapon     = @HideWeapon,
                                    HidePlayers    = @HidePlayers;";
                                    upsertCommand = new MySqlCommand(upsertQuery, (MySqlConnection)connection);
                                    break;

                                // this one
                                case DatabaseType.PostgreSQL:
                                    upsertQuery = $@"
                                    INSERT INTO ""{PlayerStatsTable}""
                                        (""PlayerName"", ""SteamID"", ""TimesConnected"", ""LastConnected"", ""HideTimerHud"", ""HideKeys"", ""SoundsEnabled"", ""PlayerFov"", ""IsVip"", ""BigGifID"", ""GlobalPoints"", ""HideWeapon"", ""HidePlayers"")
                                    VALUES
                                        (@PlayerName, @SteamID, @TimesConnected, @LastConnected, @HideTimerHud, @HideKeys, @SoundsEnabled, @PlayerFov, @IsVip, @BigGifID, @GlobalPoints, @HideWeapon, @HidePlayers)
                                    ON CONFLICT (""SteamID"")
                                    DO UPDATE SET
                                        ""PlayerName""     = EXCLUDED.""PlayerName"",
                                        ""TimesConnected"" = EXCLUDED.""TimesConnected"",
                                        ""LastConnected""  = EXCLUDED.""LastConnected"",
                                        ""HideTimerHud""   = EXCLUDED.""HideTimerHud"",
                                        ""HideKeys""       = EXCLUDED.""HideKeys"",
                                        ""SoundsEnabled""  = EXCLUDED.""SoundsEnabled"",
                                        ""PlayerFov""      = EXCLUDED.""PlayerFov"",
                                        ""IsVip""          = EXCLUDED.""IsVip"",
                                        ""BigGifID""       = EXCLUDED.""BigGifID"",
                                        ""HideWeapon""     = EXCLUDED.""HideWeapon"",
                                        ""HidePlayers""    = EXCLUDED.""HidePlayers"";";
                                    upsertCommand = new NpgsqlCommand(upsertQuery, (NpgsqlConnection)connection);
                                    break;

                                default:
                                    upsertQuery = null;
                                    upsertCommand = null;
                                    break;
                            }

                            using (upsertCommand)
                            {
                                // --- existing-row upsert ---
                                if (playerTimers.TryGetValue(playerSlot, out PlayerTimerInfo? value) || playerSlot == -1)
                                {
                                    upsertCommand!.AddParameterWithValue("@PlayerName",   playerName);
                                    upsertCommand!.AddParameterWithValue("@SteamID",      steamId);
                                    upsertCommand!.AddParameterWithValue("@TimesConnected", timesConnected);
                                    upsertCommand!.AddParameterWithValue("@LastConnected",  lastConnected);

                                    upsertCommand!.AddParameterWithValue("@HideTimerHud",  playerSlot != -1 && value!.HideTimerHud);
                                    upsertCommand!.AddParameterWithValue("@HideKeys",      playerSlot != -1 && value!.HideKeys);
                                    upsertCommand!.AddParameterWithValue("@HideWeapon",    playerSlot != -1 && value!.HideWeapon);
                                    upsertCommand!.AddParameterWithValue("@HidePlayers",   playerSlot != -1 && value!.HidePlayers);
                                    upsertCommand!.AddParameterWithValue("@SoundsEnabled", playerSlot != -1 && value!.SoundsEnabled);

                                    upsertCommand!.AddParameterWithValue("@PlayerFov",     playerSlot == -1 ? 0 : value!.PlayerFov);
                                    upsertCommand!.AddParameterWithValue("@HudType",       playerSlot == -1 ? 1 : value!.CurrentHudType);

                                    upsertCommand!.AddParameterWithValue("@IsVip",   isVip);
                                    upsertCommand!.AddParameterWithValue("@BigGifID", bigGif);
                                    //upsertCommand!.AddParameterWithValue("@GlobalPoints", playerPoints);

                                    await upsertCommand!.ExecuteNonQueryAsync();
                                    Server.NextFrame(() => SharpTimerDebug($"Set player stats in database for {playerName}"));
                                }
                                else
                                {
                                    SharpTimerError($"Error setting player stats in database for {playerName}: player was not on the server anymore");

                                    return;
                                }
                            }

                        }
                        else
                        {
                            Server.NextFrame(() => SharpTimerDebug($"No player stats yet"));
                            await row.CloseAsync();

                            string? upsertQuery;
                            DbCommand? upsertCommand;
                            switch (dbType)
                            {
                                case DatabaseType.MySQL:
                                    upsertQuery = $"REPLACE INTO {PlayerStatsTable} (PlayerName, SteamID, TimesConnected, LastConnected, HideTimerHud, HideKeys, SoundsEnabled, PlayerFov, HudType, IsVip, BigGifID, GlobalPoints, HideWeapon, HidePlayers) VALUES (@PlayerName, @SteamID, @TimesConnected, @LastConnected, @HideTimerHud, @HideKeys, @SoundsEnabled, @PlayerFov, @HudType, @IsVip, @BigGifID, @GlobalPoints, @HideWeapon, @HidePlayers)";
                                    upsertCommand = new MySqlCommand(upsertQuery, (MySqlConnection)connection);
                                    break;
                                case DatabaseType.PostgreSQL:
                                    upsertQuery = $@"INSERT INTO ""{PlayerStatsTable}"" (""PlayerName"", ""SteamID"", ""TimesConnected"", ""LastConnected"", ""HideTimerHud"", ""HideKeys"", ""SoundsEnabled"", ""PlayerFov"", ""IsVip"", ""BigGifID"", ""GlobalPoints"", ""HideWeapon"", ""HidePlayers"") VALUES (@PlayerName, @SteamID, @TimesConnected, @LastConnected, @HideTimerHud, @HideKeys, @SoundsEnabled, @PlayerFov, @IsVip, @BigGifID, @GlobalPoints, @HideWeapon, @HidePlayers)";
                                    upsertCommand = new NpgsqlCommand(upsertQuery, (NpgsqlConnection)connection);
                                    break;
                                default:
                                    upsertQuery = null;
                                    upsertCommand = null;
                                    break;
                            }

                            using (upsertCommand)
                            {
                                // --- no-row-yet insert/replace ---
                                if (playerTimers.TryGetValue(playerSlot, out PlayerTimerInfo? value) || playerSlot == -1)
                                {
                                    upsertCommand!.AddParameterWithValue("@PlayerName",   playerName);
                                    upsertCommand!.AddParameterWithValue("@SteamID",      steamId);
                                    upsertCommand!.AddParameterWithValue("@TimesConnected", 1);
                                    upsertCommand!.AddParameterWithValue("@LastConnected",  timeNowUnix);

                                    upsertCommand!.AddParameterWithValue("@HideTimerHud",  playerSlot != -1 && value!.HideTimerHud);
                                    upsertCommand!.AddParameterWithValue("@HideKeys",      playerSlot != -1 && value!.HideKeys);
                                    upsertCommand!.AddParameterWithValue("@HideWeapon",    playerSlot != -1 && value!.HideWeapon);
                                    upsertCommand!.AddParameterWithValue("@HidePlayers",   playerSlot != -1 && value!.HidePlayers);
                                    upsertCommand!.AddParameterWithValue("@SoundsEnabled", playerSlot != -1 && value!.SoundsEnabled);

                                    upsertCommand!.AddParameterWithValue("@PlayerFov",     playerSlot == -1 ? 0 : value!.PlayerFov);
                                    upsertCommand!.AddParameterWithValue("@HudType",       playerSlot == -1 ? 1 : value!.CurrentHudType);

                                    upsertCommand!.AddParameterWithValue("@IsVip",   false);
                                    upsertCommand!.AddParameterWithValue("@BigGifID", "x");
                                    upsertCommand!.AddParameterWithValue("@GlobalPoints", 0);

                                    await upsertCommand!.ExecuteNonQueryAsync();
                                    Server.NextFrame(() => SharpTimerDebug($"Set player stats in database for {playerName}"));
                                }
                                else
                                {
                                    SharpTimerError($"Error setting player stats in database for {playerName}: player was not on the server anymore");

                                    return;
                                }
                            }

                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Server.NextFrame(() => SharpTimerError($"Error setting player stats in database for {playerName}: {ex}"));
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

        public (string, int) FixMapAndBonus(string mapName)
        {
            string pattern = @"_bonus(\d+)$";
            Match match = Regex.Match(mapName, pattern);

            if (match.Success)
            {
                int bonusNumber = int.Parse(match.Groups[1].Value);
                string fixedMapName = Regex.Replace(mapName, pattern, "");

                return (fixedMapName, bonusNumber);
            }

            // Unchanged if map name doesn't contain _bonusX from import
            return (mapName, 0);
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

                    string? selectQuery;
                    DbCommand? selectCommand;
                    switch (dbType)
                    {
                        case DatabaseType.MySQL:
                            selectQuery = $@"SELECT PlayerName, TimesConnected, IsVip, BigGifID, GlobalPoints FROM {PlayerStatsTable} WHERE SteamID = @SteamID";
                            selectCommand = new MySqlCommand(selectQuery, (MySqlConnection)connection);
                            break;
                        case DatabaseType.PostgreSQL:
                            selectQuery = $@"SELECT ""PlayerName"", ""TimesConnected"", ""IsVip"", ""BigGifID"", ""GlobalPoints"" FROM ""{PlayerStatsTable}"" WHERE ""SteamID"" = @SteamID";
                            selectCommand = new NpgsqlCommand(selectQuery, (NpgsqlConnection)connection);
                            break;
                        default:
                            selectQuery = null;
                            selectCommand = null;
                            break;
                    }

                    using (selectCommand)
                    {
                        if (import)
                            selectCommand!.CommandTimeout = 120;

                        selectCommand!.AddParameterWithValue("@SteamID", steamId);

                        var row = await selectCommand!.ExecuteReaderAsync();

                        if (row.Read())
                        {
                            // get player columns
                            switch (dbType)
                            {
                                case DatabaseType.MySQL:
                                case DatabaseType.PostgreSQL:
                                    timesConnected = row.GetInt32("TimesConnected");
                                    isVip = row.GetBoolean("IsVip");
                                    bigGif = row.GetString("BigGifID");
                                    playerPoints = row.GetInt32("GlobalPoints");
                                    break;
                            }

                            int newPoints = await CalculatePlayerPoints(steamId, playerName, timerTicks, oldTicks, beatPB, bonusX, style, completions, mapname, false) + playerPoints;

                            await row.CloseAsync();
                            // Update or insert the record

                            string? upsertQuery;
                            DbCommand? upsertCommand;
                            switch (dbType)
                            {
                                case DatabaseType.MySQL:
                                    upsertQuery = $@"
                                    INSERT INTO {PlayerStatsTable}
                                        (PlayerName, SteamID, TimesConnected, LastConnected, HideTimerHud, HideKeys, SoundsEnabled, PlayerFov, HudType, IsVip, BigGifID, GlobalPoints, HideWeapon, HidePlayers)
                                    VALUES
                                        (@PlayerName, @SteamID, @TimesConnected, @LastConnected, @HideTimerHud, @HideKeys, @SoundsEnabled, @PlayerFov, @HudType, @IsVip, @BigGifID, @GlobalPoints, @HideWeapon, @HidePlayers)
                                    ON DUPLICATE KEY UPDATE
                                        PlayerName     = @PlayerName,
                                        TimesConnected = @TimesConnected,
                                        LastConnected  = @LastConnected,
                                        HideTimerHud   = @HideTimerHud,
                                        HideKeys       = @HideKeys,
                                        SoundsEnabled  = @SoundsEnabled,
                                        PlayerFov      = @PlayerFov,
                                        HudType        = @HudType,
                                        IsVip          = @IsVip,
                                        BigGifID       = @BigGifID,
                                        GlobalPoints   = @GlobalPoints,
                                        HideWeapon     = @HideWeapon,
                                        HidePlayers    = @HidePlayers;";
                                    upsertCommand = new MySqlCommand(upsertQuery, (MySqlConnection)connection);
                                    break;

                                case DatabaseType.PostgreSQL:
                                    upsertQuery = $@"
                                    INSERT INTO ""{PlayerStatsTable}""
                                        (""PlayerName"", ""SteamID"", ""TimesConnected"", ""LastConnected"", ""HideTimerHud"", ""HideKeys"", ""SoundsEnabled"", ""PlayerFov"", ""IsVip"", ""BigGifID"", ""GlobalPoints"", ""HideWeapon"", ""HidePlayers"")
                                    VALUES
                                        (@PlayerName, @SteamID, @TimesConnected, @LastConnected, @HideTimerHud, @HideKeys, @SoundsEnabled, @PlayerFov, @IsVip, @BigGifID, @GlobalPoints, @HideWeapon, @HidePlayers)
                                    ON CONFLICT (""SteamID"")
                                    DO UPDATE SET
                                        ""PlayerName""     = EXCLUDED.""PlayerName"",
                                        ""TimesConnected"" = EXCLUDED.""TimesConnected"",
                                        ""LastConnected""  = EXCLUDED.""LastConnected"",
                                        ""HideTimerHud""   = EXCLUDED.""HideTimerHud"",
                                        ""HideKeys""       = EXCLUDED.""HideKeys"",
                                        ""SoundsEnabled""  = EXCLUDED.""SoundsEnabled"",
                                        ""PlayerFov""      = EXCLUDED.""PlayerFov"",
                                        ""IsVip""          = EXCLUDED.""IsVip"",
                                        ""BigGifID""       = EXCLUDED.""BigGifID"",
                                        ""GlobalPoints""   = EXCLUDED.""GlobalPoints"",
                                        ""HideWeapon""     = EXCLUDED.""HideWeapon"",
                                        ""HidePlayers""    = EXCLUDED.""HidePlayers"";";
                                    upsertCommand = new NpgsqlCommand(upsertQuery, (NpgsqlConnection)connection);
                                    break;

                                default:
                                    upsertQuery = null;
                                    upsertCommand = null;
                                    break;
                            }

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

                            string? upsertQuery;
                            DbCommand? upsertCommand;
                            switch (dbType)
                            {
                                case DatabaseType.MySQL:
                                    upsertQuery = $@"REPLACE INTO {PlayerStatsTable} (PlayerName, SteamID, TimesConnected, LastConnected, HideTimerHud, HideKeys, SoundsEnabled, PlayerFov, HudType, IsVip, BigGifID, GlobalPoints, HideWeapon, HidePlayers) VALUES (@PlayerName, @SteamID, @TimesConnected, @LastConnected, @HideTimerHud, @HideKeys, @SoundsEnabled, @PlayerFov, @HudType, @IsVip, @BigGifID, @GlobalPoints, @HideWeapon, @HidePlayers)";
                                    upsertCommand = new MySqlCommand(upsertQuery, (MySqlConnection)connection);
                                    break;
                                case DatabaseType.PostgreSQL:
                                    upsertQuery = $@"INSERT INTO ""{PlayerStatsTable}"" (""PlayerName"", ""SteamID"", ""TimesConnected"", ""LastConnected"", ""HideTimerHud"", ""HideKeys"", ""SoundsEnabled"", ""PlayerFov"", ""IsVip"", ""BigGifID"", ""GlobalPoints"", ""HideWeapon"", ""HidePlayers"") VALUES (@PlayerName, @SteamID, @TimesConnected, @LastConnected, @HideTimerHud, @HideKeys, @SoundsEnabled, @PlayerFov, @IsVip, @BigGifID, @GlobalPoints, @HideWeapon, @HidePlayers)";
                                    upsertCommand = new NpgsqlCommand(upsertQuery, (NpgsqlConnection)connection);
                                    break;
                                default:
                                    upsertQuery = null;
                                    upsertCommand = null;
                                    break;
                            }

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

        public async Task<int> PlayerCompletions(string steamId, int bonusX = 0, int style = 0, string mapname = "")
        {
            try
            {
                // Build the exact key used in PlayerRecords.MapName
                string key;
                if (string.IsNullOrEmpty(mapname))
                {
                    key = bonusX == 0 ? currentMapName! : $"{currentMapName}_bonus{bonusX}";
                }
                else
                {
                    // If mapname already has a bonus suffix, use it as-is
                    if (Regex.IsMatch(mapname, @"_bonus\d+$", RegexOptions.IgnoreCase))
                        key = mapname;
                    else
                        key = bonusX > 0 ? $"{mapname}_bonus{bonusX}" : mapname;
                }

                using (var connection = await OpenConnectionAsync())
                {
                    await CreatePlayerRecordsTableAsync(connection);

                    string? selectQuery;
                    DbCommand? selectCommand;
                    switch (dbType)
                    {
                        case DatabaseType.MySQL:
                            selectQuery = @"SELECT TimesFinished FROM PlayerRecords WHERE MapName = @MapName AND SteamID = @SteamID AND Style = @Style";
                            selectCommand = new MySqlCommand(selectQuery, (MySqlConnection)connection);
                            break;
                        case DatabaseType.PostgreSQL:
                            selectQuery = @"SELECT ""TimesFinished"" FROM ""PlayerRecords"" WHERE ""MapName"" = @MapName AND ""SteamID"" = @SteamID AND ""Style"" = @Style";
                            selectCommand = new NpgsqlCommand(selectQuery, (NpgsqlConnection)connection);
                            break;
                        default:
                            selectQuery = null;
                            selectCommand = null;
                            break;
                    }

                    selectCommand!.AddParameterWithValue("@MapName", key);
                    selectCommand!.AddParameterWithValue("@SteamID", steamId);
                    selectCommand!.AddParameterWithValue("@Style", style);

                    using var row = await selectCommand!.ExecuteReaderAsync();
                    if (row.Read())
                        return row.GetInt32("TimesFinished");
                }
            }
            catch (Exception ex)
            {
                Server.NextFrame(() => SharpTimerError($"Error getting player completions from database for id:{steamId}: {ex}"));
            }
            return 0;
        }

        public async Task PrintTop10PlayerPoints(CCSPlayerController player)
        {
            try
            {
                using (IDbConnection connection = await OpenConnectionAsync())
                {
                    try
                    {
                        string? query;
                        DbCommand? command;
                        switch (dbType)
                        {
                            case DatabaseType.MySQL:
                                query = $@"SELECT PlayerName, GlobalPoints FROM {PlayerStatsTable} ORDER BY GlobalPoints DESC LIMIT 10";
                                command = new MySqlCommand(query, (MySqlConnection)connection);
                                break;
                            case DatabaseType.PostgreSQL:
                                query = $@"SELECT ""PlayerName"", ""GlobalPoints"" FROM ""{PlayerStatsTable}"" ORDER BY ""GlobalPoints"" DESC LIMIT 10";
                                command = new NpgsqlCommand(query, (NpgsqlConnection)connection);
                                break;
                            default:
                                query = null;
                                command = null;
                                break;
                        }

                        using (command)
                        {
                            using (DbDataReader reader = await command!.ExecuteReaderAsync())
                            {
                                Server.NextFrame(() =>
                                {
                                    if (IsAllowedClient(player)) PrintToChat(player, Localizer["top_10_points"]);
                                });

                                int rank = 0;

                                while (await reader.ReadAsync())
                                {
                                    string playerName = reader["PlayerName"].ToString()!;
                                    int points = Convert.ToInt32(reader["GlobalPoints"]);

                                    if (points >= minGlobalPointsForRank)
                                    {
                                        int currentRank = ++rank;
                                        Server.NextFrame(() =>
                                        {
                                            if (IsAllowedClient(player)) PrintToChat(player, Localizer["top_10_points_list", currentRank, playerName, points]);
                                        });
                                    }
                                }
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        Server.NextFrame(() => SharpTimerError($"An error occurred in PrintTop10PlayerPoints inside using con: {ex}"));
                    }
                }
            }
            catch (Exception ex)
            {
                Server.NextFrame(() => SharpTimerError($"An error occurred in PrintTop10PlayerPoints: {ex}"));
            }
        }

        public async Task GetReplayVIPGif(string steamId, int playerSlot)
        {
            Server.NextFrame(() => SharpTimerDebug($"Trying to get replay VIP Gif from database"));
            try
            {
                if (await IsSteamIDaTester(steamId))
                {
                    playerTimers[playerSlot].VipReplayGif = await GetTesterBigGif(steamId);
                    return;
                }

                using (var connection = await OpenConnectionAsync())
                {
                    await CreatePlayerStatsTableAsync(connection);
                    string? selectQuery;
                    DbCommand? selectCommand;
                    switch (dbType)
                    {
                        case DatabaseType.MySQL:
                            selectQuery = selectQuery = $"SELECT IsVip, BigGifID FROM {PlayerStatsTable} WHERE SteamID = @SteamID";
                            selectCommand = new MySqlCommand(selectQuery, (MySqlConnection)connection);
                            break;
                        case DatabaseType.PostgreSQL:
                            selectQuery = $@"SELECT ""IsVip"", ""BigGifID"" FROM ""{PlayerStatsTable}"" WHERE ""SteamID"" = @SteamID";
                            selectCommand = new NpgsqlCommand(selectQuery, (NpgsqlConnection)connection);
                            break;
                        default:
                            selectQuery = null;
                            selectCommand = null;
                            break;
                    }

                    using (selectCommand)
                    {
                        selectCommand!.AddParameterWithValue("@SteamID", steamId);

                        var row = await selectCommand!.ExecuteReaderAsync();

                        if (row.Read() && playerTimers.TryGetValue(playerSlot, out PlayerTimerInfo? value))
                        {
                            // get player columns
                            bool isVip = false;
                            switch (dbType)
                            {
                                case DatabaseType.MySQL:
                                case DatabaseType.PostgreSQL:
                                    isVip = row.GetBoolean("IsVip");
                                    break;
                            }
                            if (isVip)
                            {
                                Server.NextFrame(() => SharpTimerDebug($"Replay is VIP setting gif..."));
                                value.VipReplayGif = $"<br><img src='https://files.catbox.moe/{row.GetString("BigGifID")}.gif'><br>";
                            }
                            else
                            {
                                Server.NextFrame(() => SharpTimerDebug($"Replay is not VIP..."));
                                value.VipReplayGif = "x";
                            }

                            await row.CloseAsync();
                        }
                        else
                        {
                            await row.CloseAsync();
                            Server.NextFrame(() => SharpTimerDebug($"Replay is not VIP... goofy"));
                        }

                    }
                }
            }
            catch (Exception ex)
            {
                Server.NextFrame(() => SharpTimerError($"Error getting ReplayVIPGif from database: {ex}"));
            }
        }

        public async Task<(string, string, string)> GetMapRecordSteamIDFromDatabase(int bonusX = 0, int top10 = 0, int style = 0)
        {
            SharpTimerDebug($"Trying to get {(bonusX != 0 ? $"bonus {bonusX}" : "map")} record steamid from database");
            try
            {
                using (IDbConnection connection = await OpenConnectionAsync())
                {
                    await CreatePlayerRecordsTableAsync(connection);
                    string? selectQuery;
                    DbCommand? selectCommand;
                    if (top10 != 0)
                    {
                        switch (dbType)
                        {
                            case DatabaseType.MySQL:
                                // Get the top N records based on TimerTicks
                                selectQuery = "SELECT SteamID, PlayerName, TimerTicks " +
                                                "FROM PlayerRecords " +
                                                "WHERE MapName = @MapName " +
                                                "AND Style = @Style " +
                                                "ORDER BY TimerTicks ASC " +
                                                $"LIMIT 1 OFFSET {top10 - 1};";
                                selectCommand = new MySqlCommand(selectQuery, (MySqlConnection)connection);
                                break;
                            case DatabaseType.PostgreSQL:
                                // Get the top N records based on TimerTicks
                                selectQuery = @"SELECT ""SteamID"", ""PlayerName"", ""TimerTicks"" " +
                                                @"FROM ""PlayerRecords"" " +
                                                @"WHERE ""MapName"" = @MapName " +
                                                @"AND ""Style"" = @Style " +
                                                @"ORDER BY ""TimerTicks"" ASC " +
                                                $"LIMIT 1 OFFSET {top10 - 1};";
                                selectCommand = new NpgsqlCommand(selectQuery, (NpgsqlConnection)connection);
                                break;
                            default:
                                selectQuery = null;
                                selectCommand = null;
                                break;
                        }
                    }
                    else
                    {
                        // Get the overall top player
                        switch (dbType)
                        {
                            case DatabaseType.MySQL:
                                selectQuery = $"SELECT SteamID, PlayerName, TimerTicks FROM PlayerRecords WHERE MapName = @MapName AND Style = @Style ORDER BY TimerTicks ASC LIMIT 1";
                                selectCommand = new MySqlCommand(selectQuery, (MySqlConnection)connection);
                                break;
                            case DatabaseType.PostgreSQL:
                                selectQuery = $@"SELECT ""SteamID"", ""PlayerName"", ""TimerTicks"" FROM ""PlayerRecords"" WHERE ""MapName"" = @MapName AND ""Style"" = @Style ORDER BY ""TimerTicks"" ASC LIMIT 1";
                                selectCommand = new NpgsqlCommand(selectQuery, (NpgsqlConnection)connection);
                                break;
                            default:
                                selectQuery = null;
                                selectCommand = null;
                                break;
                        }
                    }

                    using (selectCommand)
                    {
                        selectCommand!.AddParameterWithValue("@MapName", bonusX == 0 ? currentMapName! : $"{currentMapName}_bonus{bonusX}");
                        selectCommand!.AddParameterWithValue("@Style", style);

                        var row = await selectCommand!.ExecuteReaderAsync();

                        if (row.Read())
                        {
                            string steamId64 = row.GetString("SteamID");
                            string playerName = row.GetString("PlayerName");
                            string timerTicks = FormatTime(row.GetInt32("TimerTicks"));


                            await row.CloseAsync();

                            return (steamId64, playerName, timerTicks);
                        }
                        else
                        {
                            await row.CloseAsync();

                            return ("null", "null", "null");
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Server.NextFrame(() => SharpTimerError($"Error getting GetMapRecordSteamIDFromDatabase from database: {ex}"));
                return ("null", "null", "null");
            }
        }
        public async Task<(string, string, string)> GetStageRecordSteamIDFromDatabase(int stage, int bonusX = 0, int top10 = 0)
        {
            SharpTimerDebug($"Trying to get {(bonusX != 0 ? $"bonus {bonusX} stage {stage}" : $"stage {stage}")} record steamid from database");
            try
            {
                using (IDbConnection connection = await OpenConnectionAsync())
                {
                    string? selectQuery;
                    DbCommand? selectCommand;
                    if (top10 != 0)
                    {
                        switch (dbType)
                        {
                            case DatabaseType.MySQL:
                                // Get the top N records based on TimerTicks
                                selectQuery = "SELECT SteamID, PlayerName, TimerTicks " +
                                                "FROM PlayerStageTimes " +
                                                "WHERE MapName = @MapName " +
                                                "AND Stage = @Stage " +
                                                "ORDER BY TimerTicks ASC " +
                                                $"LIMIT 1 OFFSET {top10 - 1};";
                                selectCommand = new MySqlCommand(selectQuery, (MySqlConnection)connection);
                                break;
                            case DatabaseType.PostgreSQL:
                                // Get the top N records based on TimerTicks
                                selectQuery = @"SELECT ""SteamID"", ""PlayerName"", ""TimerTicks"" " +
                                                @"FROM ""PlayerStageTimes"" " +
                                                @"WHERE ""MapName"" = @MapName " +
                                                @"AND ""Stage"" = @Stage " +
                                                @"ORDER BY ""TimerTicks"" ASC " +
                                                $"LIMIT 1 OFFSET {top10 - 1};";
                                selectCommand = new NpgsqlCommand(selectQuery, (NpgsqlConnection)connection);
                                break;
                            default:
                                selectQuery = null;
                                selectCommand = null;
                                break;
                        }
                    }
                    else
                    {
                        // Get the overall top player
                        switch (dbType)
                        {
                            case DatabaseType.MySQL:
                                selectQuery = $"SELECT SteamID, PlayerName, TimerTicks FROM PlayerStageTimes WHERE MapName = @MapName AND Stage = @Stage ORDER BY TimerTicks ASC LIMIT 1";
                                selectCommand = new MySqlCommand(selectQuery, (MySqlConnection)connection);
                                break;
                            case DatabaseType.PostgreSQL:
                                selectQuery = $@"SELECT ""SteamID"", ""PlayerName"", ""TimerTicks"" FROM ""PlayerStageTimes"" WHERE ""MapName"" = @MapName AND ""Stage"" = @Stage ORDER BY ""TimerTicks"" ASC LIMIT 1";
                                selectCommand = new NpgsqlCommand(selectQuery, (NpgsqlConnection)connection);
                                break;
                            default:
                                selectQuery = null;
                                selectCommand = null;
                                break;
                        }
                    }

                    using (selectCommand)
                    {
                        selectCommand!.AddParameterWithValue("@MapName", bonusX == 0 ? currentMapName! : $"{currentMapName}_bonus{bonusX}");
                        selectCommand!.AddParameterWithValue("@Stage", stage);

                        var row = await selectCommand!.ExecuteReaderAsync();

                        if (row.Read())
                        {
                            string steamId64 = row.GetString("SteamID");
                            string playerName = row.GetString("PlayerName");
                            string timerTicks = FormatTime(row.GetInt32("TimerTicks"));


                            await row.CloseAsync();

                            return (steamId64, playerName, timerTicks);
                        }
                        else
                        {
                            await row.CloseAsync();

                            return ("null", "null", "null");
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Server.NextFrame(() => SharpTimerError($"Error getting GetStageRecordSteamIDFromDatabase from database: {ex}"));
                return ("null", "null", "null");
            }
        }

        public async Task<(int, string)> GetStageRecordFromDatabase(int stage, string steamId, int bonusX = 0)
        {
            SharpTimerDebug($"Trying to get {(bonusX != 0 ? $"bonus {bonusX} stage {stage}" : $"stage {stage}")} record steamid from database");
            try
            {
                using (IDbConnection connection = await OpenConnectionAsync())
                {
                    string? selectQuery;
                    DbCommand? selectCommand;
                    switch (dbType)
                    {
                        case DatabaseType.MySQL:
                            // Get the top N records based on TimerTicks
                            selectQuery = "SELECT Velocity, TimerTicks " +
                                            "FROM PlayerStageTimes " +
                                            "WHERE MapName = @MapName " +
                                            "AND Stage = @Stage " +
                                            "AND SteamID = @SteamID " +
                                            "ORDER BY TimerTicks ASC " +
                                            $"LIMIT 1;";
                            selectCommand = new MySqlCommand(selectQuery, (MySqlConnection)connection);
                            break;
                        case DatabaseType.PostgreSQL:
                            // Get the top N records based on TimerTicks
                            selectQuery = @"SELECT ""Velocity"", ""TimerTicks"" " +
                                            @"FROM ""PlayerStageTimes"" " +
                                            @"WHERE ""MapName"" = @MapName " +
                                            @"AND ""Stage"" = @Stage " +
                                            @"AND ""SteamID"" = @SteamID " +
                                            @"ORDER BY ""TimerTicks"" ASC " +
                                            $"LIMIT 1;";
                            selectCommand = new NpgsqlCommand(selectQuery, (NpgsqlConnection)connection);
                            break;
                        default:
                            selectQuery = null;
                            selectCommand = null;
                            break;
                    }

                    using (selectCommand)
                    {
                        selectCommand!.AddParameterWithValue("@MapName", bonusX == 0 ? currentMapName! : $"{currentMapName}_bonus{bonusX}");
                        selectCommand!.AddParameterWithValue("@Stage", stage);
                        selectCommand!.AddParameterWithValue("@SteamID", steamId);

                        var row = await selectCommand!.ExecuteReaderAsync();

                        if (row.Read())
                        {
                            int stageTime = row.GetInt32("TimerTicks");
                            string stageSpeed = row.GetString("Velocity");


                            await row.CloseAsync();

                            return (stageTime, stageSpeed);
                        }
                        else
                        {
                            await row.CloseAsync();

                            return (0, "null");
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Server.NextFrame(() => SharpTimerError($"Error getting GetStageRecord from database: {ex}"));
                return (0, "null");
            }
        }

        public async Task<int> GetPreviousPlayerRecordFromDatabase(string steamId, string currentMapName, string playerName, int bonusX = 0, int style = 0)
        {
            SharpTimerDebug($"Trying to get Previous {(bonusX != 0 ? $"bonus {bonusX} time" : "time")} from database for {playerName}");
            try
            {
                string currentMapNamee = bonusX == 0 ? currentMapName : $"{currentMapName}_bonus{bonusX}";

                using (IDbConnection connection = await OpenConnectionAsync())
                {
                    await CreatePlayerRecordsTableAsync(connection);
                    string? selectQuery;
                    DbCommand? selectCommand;

                    // Retrieve the TimerTicks value for the specified player on the current map
                    switch (dbType)
                    {
                        case DatabaseType.MySQL:
                            selectQuery = "SELECT TimerTicks FROM PlayerRecords WHERE MapName = @MapName AND SteamID = @SteamID AND Style = @Style";
                            selectCommand = new MySqlCommand(selectQuery, (MySqlConnection)connection);
                            break;
                        case DatabaseType.PostgreSQL:
                            selectQuery = @"SELECT ""TimerTicks"" FROM ""PlayerRecords"" WHERE ""MapName"" = @MapName AND ""SteamID"" = @SteamID AND ""Style"" = @Style";
                            selectCommand = new NpgsqlCommand(selectQuery, (NpgsqlConnection)connection);
                            break;
                        default:
                            selectQuery = null;
                            selectCommand = null;
                            break;
                    }

                    using (selectCommand)
                    {
                        selectCommand!.AddParameterWithValue("@MapName", currentMapNamee);
                        selectCommand!.AddParameterWithValue("@SteamID", steamId);
                        selectCommand!.AddParameterWithValue("@Style", style);

                        var result = await selectCommand!.ExecuteScalarAsync();

                        // Check for DBNull
                        if (result != null && result != DBNull.Value)
                        {
                            SharpTimerDebug($"Got Previous Time from database for {playerName}");
                            return Convert.ToInt32(result);
                        }
                    }

                }
            }
            catch (Exception ex)
            {
                SharpTimerError($"Error getting previous player {(bonusX != 0 ? $"bonus {bonusX} time" : "time")} from database: {ex.Message}");
            }

            return 0;
        }
        public async Task<int> GetPreviousPlayerStageRecordFromDatabase(CCSPlayerController? player, string steamId, string currentMapName, int stage, string playerName, int bonusX = 0)
        {
            SharpTimerDebug($"Trying to get Previous {(bonusX != 0 ? $"bonus {bonusX} stage {stage} time" : $"stage {stage} time")} from database for {playerName}");
            try
            {
                if (!IsAllowedClient(player))
                {
                    return 0;
                }

                string currentMapNamee = bonusX == 0 ? currentMapName : $"{currentMapName}_bonus{bonusX}";

                using (IDbConnection connection = await OpenConnectionAsync())
                {
                    string? selectQuery;
                    DbCommand? selectCommand;

                    // Retrieve the TimerTicks value for the specified player on the current map
                    switch (dbType)
                    {
                        case DatabaseType.MySQL:
                            selectQuery = "SELECT TimerTicks FROM PlayerStageTimes WHERE MapName = @MapName AND SteamID = @SteamID AND Stage = @Stage";
                            selectCommand = new MySqlCommand(selectQuery, (MySqlConnection)connection);
                            break;
                        case DatabaseType.PostgreSQL:
                            selectQuery = @"SELECT ""TimerTicks"" FROM ""PlayerStageTimes"" WHERE ""MapName"" = @MapName AND ""SteamID"" = @SteamID AND ""Stage"" = @Stage";
                            selectCommand = new NpgsqlCommand(selectQuery, (NpgsqlConnection)connection);
                            break;
                        default:
                            selectQuery = null;
                            selectCommand = null;
                            break;
                    }

                    using (selectCommand)
                    {
                        selectCommand!.AddParameterWithValue("@MapName", currentMapNamee);
                        selectCommand!.AddParameterWithValue("@SteamID", steamId);
                        selectCommand!.AddParameterWithValue("@Stage", stage);

                        var result = await selectCommand!.ExecuteScalarAsync();

                        // Check for DBNull
                        if (result != null && result != DBNull.Value)
                        {
                            SharpTimerDebug($"Got Previous stage {stage} Time from database for {playerName}");
                            return Convert.ToInt32(result);
                        }
                    }

                }
            }
            catch (Exception ex)
            {
                SharpTimerError($"Error getting previous player {(bonusX != 0 ? $"bonus {bonusX} stage {stage} time" : $"stage {stage} time")} from database: {ex.Message}");
            }

            return 0;
        }

        public async Task<int> GetPlayerPointsFromDatabase(string steamId, string? playerName = null)
        {
            SharpTimerDebug("Trying GetPlayerPointsFromDatabase");
            int playerPoints = 0;

            try
            {
                using (var connection = await OpenConnectionAsync())
                {
                    await CreatePlayerStatsTableAsync(connection);
                    string? selectQuery;
                    DbCommand? selectCommand;
                    switch (dbType)
                    {
                        case DatabaseType.MySQL:
                            selectQuery = $"SELECT GlobalPoints FROM {PlayerStatsTable} WHERE SteamID = @SteamID";
                            selectCommand = new MySqlCommand(selectQuery, (MySqlConnection)connection);
                            break;
                        case DatabaseType.PostgreSQL:
                            selectQuery = $@"SELECT ""GlobalPoints"" FROM ""{PlayerStatsTable}"" WHERE ""SteamID"" = @SteamID";
                            selectCommand = new NpgsqlCommand(selectQuery, (NpgsqlConnection)connection);
                            break;
                        default:
                            selectQuery = null;
                            selectCommand = null;
                            break;
                    }

                    using (selectCommand)
                    {
                        selectCommand!.AddParameterWithValue("@SteamID", steamId);

                        var result = await selectCommand!.ExecuteScalarAsync();

                        // Check for DBNull
                        if (result != null && result != DBNull.Value)
                        {
                            playerPoints = Convert.ToInt32(result);
                            SharpTimerDebug($"Got Player Points from database for {playerName} p: {playerPoints}");
                            return playerPoints;
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                SharpTimerError($"Error getting player points from database: {ex.Message}");
            }
            return playerPoints;
        }

        public async Task<Dictionary<int, PlayerRecord>> GetSortedRecordsFromDatabase(int limit = 0, int bonusX = 0, string mapName = "", int style = 0)
        {
            SharpTimerDebug($"Trying GetSortedRecords {(bonusX != 0 ? $"bonus {bonusX}" : "")} from database");
            using (var connection = await OpenConnectionAsync())
            {
                try
                {
                    string? currentMapNamee;
                    if (string.IsNullOrEmpty(mapName))
                        currentMapNamee = bonusX == 0 ? currentMapName! : $"{currentMapName}_bonus{bonusX}";
                    else
                        currentMapNamee = mapName;

                    await CreatePlayerRecordsTableAsync(connection);

                    // Retrieve and sort records for the current map
                    string? selectQuery;
                    DbCommand? selectCommand;
                    if (limit != 0)
                    {
                        switch (dbType)
                        {
                            case DatabaseType.MySQL:
                                selectQuery = $@"SELECT SteamID, PlayerName, TimerTicks FROM PlayerRecords WHERE MapName = @MapName AND Style = @Style ORDER BY TimerTicks ASC LIMIT {limit}";
                                selectCommand = new MySqlCommand(selectQuery, (MySqlConnection)connection);
                                break;
                            case DatabaseType.PostgreSQL:
                                selectQuery = $@"SELECT ""SteamID"", ""PlayerName"", ""TimerTicks"" FROM ""PlayerRecords"" WHERE ""MapName"" = @MapName AND ""Style"" = @Style ORDER BY ""TimerTicks"" ASC LIMIT {limit}";
                                selectCommand = new NpgsqlCommand(selectQuery, (NpgsqlConnection)connection);
                                break;
                            default:
                                selectQuery = null;
                                selectCommand = null;
                                break;
                        }
                    }
                    else
                    {
                        switch (dbType)
                        {
                            case DatabaseType.MySQL:
                                selectQuery = @"SELECT SteamID, PlayerName, TimerTicks FROM PlayerRecords WHERE MapName = @MapName AND Style = @Style";
                                selectCommand = new MySqlCommand(selectQuery, (MySqlConnection)connection);
                                break;
                            case DatabaseType.PostgreSQL:
                                selectQuery = @"SELECT ""SteamID"", ""PlayerName"", ""TimerTicks"" FROM ""PlayerRecords"" WHERE ""MapName"" = @MapName AND ""Style"" = @Style";
                                selectCommand = new NpgsqlCommand(selectQuery, (NpgsqlConnection)connection);
                                break;
                            default:
                                selectQuery = null;
                                selectCommand = null;
                                break;
                        }
                    }
                    using (selectCommand)
                    {
                        selectCommand!.AddParameterWithValue("@MapName", currentMapNamee);
                        selectCommand!.AddParameterWithValue("@Style", style);
                        using (var reader = await selectCommand!.ExecuteReaderAsync())
                        {
                            var sortedRecords = new Dictionary<int, PlayerRecord>();
                            int record = 0;
                            while (await reader.ReadAsync())
                            {
                                string steamId = reader.GetString(0);
                                string playerName = reader.IsDBNull(1) ? "Unknown" : reader.GetString(1);
                                int timerTicks = reader.GetInt32(2);
                                sortedRecords.Add(record, new PlayerRecord
                                {
                                    SteamID = steamId,
                                    PlayerName = playerName,
                                    TimerTicks = timerTicks
                                });
                                record++;
                            }

                            // Sort the records by TimerTicks
                            sortedRecords = sortedRecords.OrderBy(record => record.Value.TimerTicks)
                                                        .ToDictionary(record => record.Key, record => record.Value);

                            SharpTimerDebug($"Got GetSortedRecords {(bonusX != 0 ? $"bonus {bonusX}" : "")} from database");

                            return sortedRecords;
                        }
                    }
                }
                catch (Exception ex)
                {
                    SharpTimerError($"Error getting sorted records from database: {ex.Message}");
                }
            }
            return [];
        }
        
        public async Task<List<PlayerRecord>> GetAllSortedRecordsFromDatabase(int limit = 0, int bonusX = 0, int style = 0)
        {
            SharpTimerDebug($"Trying GetSortedRecords {(bonusX != 0 ? $"bonus {bonusX}" : "")} from database");
            using (var connection = await OpenConnectionAsync())
            {
                try
                {
                    await CreatePlayerRecordsTableAsync(connection);

                    string? selectQuery;
                    DbCommand? selectCommand;

                    if (limit != 0)
                    {
                        switch (dbType)
                        {
                            case DatabaseType.MySQL:
                                selectQuery = @"SELECT SteamID, PlayerName, TimerTicks, MapName
                                                FROM PlayerRecords
                                                WHERE Style = @Style
                                                ORDER BY TimerTicks ASC
                                                LIMIT " + limit;
                                selectCommand = new MySqlCommand(selectQuery, (MySqlConnection)connection);
                                break;
                            case DatabaseType.PostgreSQL:
                                selectQuery = @"SELECT ""SteamID"", ""PlayerName"", ""TimerTicks"", ""MapName""
                                                FROM ""PlayerRecords""
                                                WHERE ""Style"" = @Style
                                                ORDER BY ""TimerTicks"" ASC
                                                LIMIT " + limit;
                                selectCommand = new NpgsqlCommand(selectQuery, (NpgsqlConnection)connection);
                                break;
                            default:
                                selectQuery = null;
                                selectCommand = null;
                                break;
                        }
                    }
                    else
                    {
                        switch (dbType)
                        {
                            case DatabaseType.MySQL:
                                selectQuery = @"SELECT SteamID, PlayerName, TimerTicks, MapName
                                                FROM PlayerRecords
                                                WHERE Style = @Style";
                                selectCommand = new MySqlCommand(selectQuery, (MySqlConnection)connection);
                                break;
                            case DatabaseType.PostgreSQL:
                                selectQuery = @"SELECT ""SteamID"", ""PlayerName"", ""TimerTicks"", ""MapName""
                                                FROM ""PlayerRecords""
                                                WHERE ""Style"" = @Style";
                                selectCommand = new NpgsqlCommand(selectQuery, (NpgsqlConnection)connection);
                                break;
                            default:
                                selectQuery = null;
                                selectCommand = null;
                                break;
                        }
                    }

                    using (selectCommand)
                    {
                        selectCommand!.AddParameterWithValue("@Style", style);
                        using (var reader = await selectCommand!.ExecuteReaderAsync())
                        {
                            // Group by SteamID first (like your previous code), then flatten.
                            var buckets = new Dictionary<string, List<PlayerRecord>>();

                            while (await reader.ReadAsync())
                            {
                                string steamId = reader.GetString(0);
                                string playerName = reader.IsDBNull(1) ? "Unknown" : reader.GetString(1);
                                int timerTicks = reader.GetInt32(2);
                                string rawMap = reader.GetString(3);

                                // Get BonusX from the map name suffix (_bonusN)
                                var (fixedMap, bX) = FixMapAndBonus(rawMap);

                                if (!buckets.ContainsKey(steamId))
                                    buckets[steamId] = new List<PlayerRecord>();

                                buckets[steamId].Add(new PlayerRecord
                                {
                                    SteamID = steamId,
                                    PlayerName = playerName,
                                    TimerTicks = timerTicks,
                                    // Keep the original map name (with suffix) so lookups work
                                    MapName = rawMap,
                                    BonusX = bX
                                });
                            }

                            var sortedList = buckets
                                .SelectMany(kv => kv.Value)
                                .OrderBy(r => r.TimerTicks)
                                .ToList();

                            SharpTimerDebug($"Got GetSortedRecords {(bonusX != 0 ? $"bonus {bonusX}" : "")} from database");
                            return sortedList;
                        }
                    }
                }
                catch (Exception ex)
                {
                    SharpTimerError($"Error getting all sorted records from database: {ex.Message}");
                }
            }
            return [];
        }

        public async Task<Dictionary<string, PlayerRecord>> GetSortedStageRecordsFromDatabase(int stage, int limit = 0, int bonusX = 0, string mapName = "")
        {
            SharpTimerDebug($"Trying GetSortedStageRecords {(bonusX != 0 ? $"bonus {bonusX}" : "")} from database");
            using (var connection = await OpenConnectionAsync())
            {
                try
                {
                    string? currentMapNamee;
                    if (string.IsNullOrEmpty(mapName))
                        currentMapNamee = bonusX == 0 ? currentMapName! : $"{currentMapName}_bonus{bonusX}";
                    else
                        currentMapNamee = mapName;

                    await CreatePlayerRecordsTableAsync(connection);

                    // Retrieve and sort records for the current map
                    string? selectQuery;
                    DbCommand? selectCommand;
                    if (limit != 0)
                    {
                        switch (dbType)
                        {
                            case DatabaseType.MySQL:
                                selectQuery = $@"SELECT SteamID, PlayerName, TimerTicks FROM PlayerStageTimes WHERE MapName = @MapName AND Stage = @Stage ORDER BY TimerTicks ASC LIMIT {limit}";
                                selectCommand = new MySqlCommand(selectQuery, (MySqlConnection)connection);
                                break;
                            case DatabaseType.PostgreSQL:
                                selectQuery = $@"SELECT ""SteamID"", ""PlayerName"", ""TimerTicks"" FROM ""PlayerStageTimes"" WHERE ""MapName"" = @MapName AND ""Stage"" = @Stage ORDER BY ""TimerTicks"" ASC LIMIT {limit}";
                                selectCommand = new NpgsqlCommand(selectQuery, (NpgsqlConnection)connection);
                                break;
                            default:
                                selectQuery = null;
                                selectCommand = null;
                                break;
                        }
                    }
                    else
                    {
                        switch (dbType)
                        {
                            case DatabaseType.MySQL:
                                selectQuery = @"SELECT SteamID, PlayerName, TimerTicks FROM PlayerStageTimes WHERE MapName = @MapName AND Stage = @Stage";
                                selectCommand = new MySqlCommand(selectQuery, (MySqlConnection)connection);
                                break;
                            case DatabaseType.PostgreSQL:
                                selectQuery = @"SELECT ""SteamID"", ""PlayerName"", ""TimerTicks"" FROM ""PlayerStageTimes"" WHERE ""MapName"" = @MapName AND ""Stage"" = @Stage";
                                selectCommand = new NpgsqlCommand(selectQuery, (NpgsqlConnection)connection);
                                break;
                            default:
                                selectQuery = null;
                                selectCommand = null;
                                break;
                        }
                    }
                    using (selectCommand)
                    {
                        selectCommand!.AddParameterWithValue("@MapName", currentMapNamee);
                        selectCommand!.AddParameterWithValue("@Stage", stage);
                        using (var reader = await selectCommand!.ExecuteReaderAsync())
                        {
                            var sortedRecords = new Dictionary<string, PlayerRecord>();
                            while (await reader.ReadAsync())
                            {
                                string steamId = reader.GetString(0);
                                string playerName = reader.IsDBNull(1) ? "Unknown" : reader.GetString(1);
                                int timerTicks = reader.GetInt32(2);
                                sortedRecords.Add(steamId, new PlayerRecord
                                {
                                    PlayerName = playerName,
                                    TimerTicks = timerTicks
                                });
                            }

                            // Sort the records by TimerTicks
                            sortedRecords = sortedRecords.OrderBy(record => record.Value.TimerTicks)
                                                        .ToDictionary(record => record.Key, record => record.Value);

                            SharpTimerDebug($"Got GetSortedStageRecords {(bonusX != 0 ? $"bonus {bonusX}" : "")} from database");

                            return sortedRecords;
                        }
                    }
                }
                catch (Exception ex)
                {
                    SharpTimerError($"Error getting sorted stage records from database: {ex.Message}");
                }
            }
            return [];
        }

        public async Task<Dictionary<string, PlayerPoints>> GetSortedPointsFromDatabase()
        {
            SharpTimerDebug("Trying GetSortedPoints from database");
            using (var connection = await OpenConnectionAsync())
            {
                try
                {
                    await CreatePlayerStatsTableAsync(connection);
                    string? selectQuery;
                    DbCommand? selectCommand;
                    switch (dbType)
                    {
                        case DatabaseType.MySQL:
                            selectQuery = $@"SELECT SteamID, PlayerName, GlobalPoints FROM {PlayerStatsTable}";
                            selectCommand = new MySqlCommand(selectQuery, (MySqlConnection)connection);
                            break;
                        case DatabaseType.PostgreSQL:
                            selectQuery = $@"SELECT ""SteamID"", ""PlayerName"", ""GlobalPoints"" FROM ""{PlayerStatsTable}""";
                            selectCommand = new NpgsqlCommand(selectQuery, (NpgsqlConnection)connection);
                            break;
                        default:
                            selectQuery = null;
                            selectCommand = null;
                            break;
                    }

                    using (selectCommand)
                    {
                        using (var reader = await selectCommand!.ExecuteReaderAsync())
                        {
                            var sortedPoints = new Dictionary<string, PlayerPoints>();
                            while (await reader.ReadAsync())
                            {
                                string steamId = reader.GetString(0);
                                string playerName = reader.IsDBNull(1) ? "Unknown" : reader.GetString(1);
                                int globalPoints = reader.GetInt32(2);

                                if (globalPoints >= minGlobalPointsForRank)
                                {
                                    sortedPoints.Add(steamId, new PlayerPoints
                                    {
                                        PlayerName = playerName,
                                        GlobalPoints = globalPoints
                                    });
                                }
                            }

                            sortedPoints = sortedPoints.OrderByDescending(record => record.Value.GlobalPoints)
                                                        .ToDictionary(record => record.Key, record => record.Value);

                            return sortedPoints;
                        }
                    }
                }
                catch (Exception ex)
                {
                    SharpTimerError($"Error getting GetSortedPoints from database: {ex.Message}");
                }
            }
            return [];
        }

        [ConsoleCommand("css_importpoints", " ")]
        [RequiresPermissions("@css/root")]
        [CommandHelper(whoCanExecute: CommandUsage.CLIENT_AND_SERVER)]
        public void ImportPlayerPointsCommand(CCSPlayerController? player, CommandInfo command)
        {
            _ = Task.Run(ImportPlayerPoints);
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

        private async Task UpsertGlobalPointsAsync(string steamId, string playerName, int points)
        {
            using (var connection = await OpenConnectionAsync())
            {
                await CreatePlayerStatsTableAsync(connection);

                string? upsertQuery;
                DbCommand? cmd;

                switch (dbType)
                {
                    case DatabaseType.MySQL:
                        upsertQuery = $@"
                            INSERT INTO {PlayerStatsTable}
                                (PlayerName, SteamID, GlobalPoints)
                            VALUES
                                (@PlayerName, @SteamID, @Points)
                            ON DUPLICATE KEY UPDATE
                                PlayerName = VALUES(PlayerName),
                                GlobalPoints = VALUES(GlobalPoints);";
                        cmd = new MySqlCommand(upsertQuery, (MySqlConnection)connection);
                        break;

                    case DatabaseType.PostgreSQL:
                        upsertQuery = $@"
                            INSERT INTO ""{PlayerStatsTable}""
                                (""PlayerName"", ""SteamID"", ""GlobalPoints"")
                            VALUES
                                (@PlayerName, @SteamID, @Points)
                            ON CONFLICT (""SteamID"")
                            DO UPDATE SET
                                ""PlayerName""   = EXCLUDED.""PlayerName"",
                                ""GlobalPoints"" = EXCLUDED.""GlobalPoints"";";
                        cmd = new NpgsqlCommand(upsertQuery, (NpgsqlConnection)connection);
                        break;

                    default:
                        upsertQuery = null; cmd = null; break;
                }

                using (cmd)
                {
                    cmd!.AddParameterWithValue("@PlayerName", playerName);
                    cmd!.AddParameterWithValue("@SteamID", steamId);
                    cmd!.AddParameterWithValue("@Points", points);
                    await cmd!.ExecuteNonQueryAsync();
                }
            }
        }

        private async Task<int> GetTimesFinishedAsync(string steamId, string mapNameWithSuffix, int style)
        {
            using (var connection = await OpenConnectionAsync())
            {
                await CreatePlayerRecordsTableAsync(connection);

                string? q; DbCommand? cmd;
                switch (dbType)
                {
                    case DatabaseType.MySQL:
                        q = @"SELECT TimesFinished
                            FROM PlayerRecords
                            WHERE MapName = @MapName AND SteamID = @SteamID AND Style = @Style";
                        cmd = new MySqlCommand(q, (MySqlConnection)connection);
                        break;
                    case DatabaseType.PostgreSQL:
                        q = @"SELECT ""TimesFinished""
                            FROM ""PlayerRecords""
                            WHERE ""MapName"" = @MapName AND ""SteamID"" = @SteamID AND ""Style"" = @Style";
                        cmd = new NpgsqlCommand(q, (NpgsqlConnection)connection);
                        break;
                    default:
                        return 0;
                }

                using (cmd)
                {
                    cmd!.AddParameterWithValue("@MapName", mapNameWithSuffix); // e.g. "surf_aircontrol_ksf" or "..._bonus1"
                    cmd!.AddParameterWithValue("@SteamID", steamId);
                    cmd!.AddParameterWithValue("@Style", style);

                    var result = await cmd!.ExecuteScalarAsync();
                    return (result != null && result != DBNull.Value) ? Convert.ToInt32(result) : 0;
                }
            }
        }

        [ConsoleCommand("css_resetpoints", "Resets points to zero. Used before doing a points recalculation.")]
        [RequiresPermissions("@css/root")]
        [CommandHelper(whoCanExecute: CommandUsage.CLIENT_AND_SERVER)]
        public void ResetPlayerPointsCommand(CCSPlayerController? player, CommandInfo command)
        {
            _ = Task.Run(ResetPlayerPoints);
            player?.PrintToChat($"{Localizer["prefix"]} Points have been reset!");
        }

        public async Task ResetPlayerPoints()
        {
            using (var connection = await OpenConnectionAsync())
            {
                try
                {
                    await CreatePlayerStatsTableAsync(connection);
                    string? updateQuery;
                    DbCommand? updateCommand;
                    switch (dbType)
                    {
                        case DatabaseType.MySQL:
                            updateQuery = $@"UPDATE {PlayerStatsTable} SET GlobalPoints = 0";
                            updateCommand = new MySqlCommand(updateQuery, (MySqlConnection)connection);
                            break;
                        case DatabaseType.PostgreSQL:
                            updateQuery = $@"UPDATE ""{PlayerStatsTable}"" SET ""GlobalPoints"" = 0";
                            updateCommand = new NpgsqlCommand(updateQuery, (NpgsqlConnection)connection);
                            break;
                        default:
                            updateQuery = null;
                            updateCommand = null;
                            break;
                    }
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

        [ConsoleCommand("css_jsontodatabase", " ")]
        [RequiresPermissions("@css/root")]
        [CommandHelper(whoCanExecute: CommandUsage.CLIENT_AND_SERVER)]
        public void AddJsonTimesToDatabaseCommand(CCSPlayerController? player, CommandInfo command)
        {
            _ = Task.Run(AddJsonTimesToDatabaseAsync);
        }

        public async Task AddJsonTimesToDatabaseAsync()
        {
            try
            {
                string recordsDirectoryNamee = "SharpTimer/PlayerRecords";
                string playerRecordsPathh = Path.Combine(gameDir!, "csgo", "cfg", recordsDirectoryNamee);

                if (!Directory.Exists(playerRecordsPathh))
                {
                    SharpTimerDebug($"Error: Directory not found at {playerRecordsPathh}");
                    return;
                }

                string connectionString = await GetConnectionStringFromConfigFile();
                IDbConnection? connection = null;
                switch (dbType)
                {
                    case DatabaseType.MySQL:
                        connection = new MySqlConnection(connectionString);
                        break;
                    case DatabaseType.PostgreSQL:
                        connection = new NpgsqlConnection(connectionString);
                        break;
                    default:
                        SharpTimerError($"Error: Invalid database type.");
                        return;
                }
                using (connection)
                {
                    connection.Open();

                    // Check if the table exists, and create it if necessary
                    string? createTableQuery = null;
                    DbCommand? createTableCommand = null;
                    switch (dbType)
                    {
                        case DatabaseType.MySQL:
                            createTableQuery = @"CREATE TABLE IF NOT EXISTS PlayerRecords (
                                            MapName VARCHAR(255),
                                            SteamID VARCHAR(255),
                                            PlayerName VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci,
                                            TimerTicks INT,
                                            FormattedTime VARCHAR(255),
                                            UnixStamp INT,
                                            TimesFinished INT,
                                            LastFinished INT,
                                            Style INT,
                                            PRIMARY KEY (MapName, SteamID)
                                        )";
                            createTableCommand = new MySqlCommand(createTableQuery, (MySqlConnection)connection);
                            break;
                        case DatabaseType.PostgreSQL:
                            createTableQuery = @"CREATE TABLE IF NOT EXISTS ""PlayerRecords"" (
                                            ""MapName"" VARCHAR(255),
                                            ""SteamID"" VARCHAR(255),
                                            ""PlayerName"" VARCHAR(255),
                                            ""TimerTicks"" INT,
                                            ""FormattedTime"" VARCHAR(255),
                                            ""UnixStamp"" INT,
                                            ""TimesFinished"" INT,
                                            ""LastFinished"" INT,
                                            ""Style"" INT,
                                            PRIMARY KEY (""MapName"", ""SteamID"", ""Style"")
                                        )";
                            createTableCommand = new NpgsqlCommand(createTableQuery, (NpgsqlConnection)connection);
                            break;
                        default:
                            createTableQuery = null;
                            break;
                    }
                    using (createTableCommand)
                    {
                        await createTableCommand!.ExecuteNonQueryAsync();
                    }

                    foreach (var filePath in Directory.EnumerateFiles(playerRecordsPathh, "*.json"))
                    {
                        string json = await File.ReadAllTextAsync(filePath);
                        var records = JsonSerializer.Deserialize<Dictionary<string, PlayerRecord>>(json);

                        if (records == null)
                        {
                            SharpTimerDebug($"Error: Failed to deserialize JSON data from {filePath}");
                            continue;
                        }

                        foreach (var recordEntry in records)
                        {
                            string steamId = recordEntry.Key;
                            PlayerRecord playerRecord = recordEntry.Value;

                            // Extract MapName from the filename (remove extension)
                            string mapName = Path.GetFileNameWithoutExtension(filePath);

                            // Check if the player is already in the database
                            string? insertOrUpdateQuery = null;
                            DbCommand? insertOrUpdateCommand = null;
                            switch (dbType)
                            {
                                case DatabaseType.MySQL:
                                    insertOrUpdateQuery = @"INSERT INTO PlayerRecords (SteamID, PlayerName, TimerTicks, FormattedTime, MapName, UnixStamp, TimesFinished, LastFinished, Style)
                                        VALUES (@SteamID, @PlayerName, @TimerTicks, @FormattedTime, @MapName, @UnixStamp, @TimesFinished, @LastFinished, @Style)
                                        ON DUPLICATE KEY UPDATE
                                        TimerTicks = if (@TimerTicks < TimerTicks, @TimerTicks, TimerTicks),
                                        FormattedTime = if (@TimerTicks < TimerTicks, @FormattedTime, FormattedTime)";
                                    insertOrUpdateCommand = new MySqlCommand(insertOrUpdateQuery, (MySqlConnection)connection);
                                    break;
                                case DatabaseType.PostgreSQL:
                                    insertOrUpdateQuery = @"INSERT INTO ""PlayerRecords"" (""SteamID"", ""PlayerName"", ""TimerTicks"", ""FormattedTime"", ""MapName"", ""UnixStamp"", ""TimesFinished"", ""LastFinished"", ""Style"")
                                        VALUES (@SteamID, @PlayerName, @TimerTicks, @FormattedTime, @MapName, @UnixStamp, @TimesFinished, @LastFinished, @Style)
                                        ON CONFLICT (""MapName"", ""SteamID"", ""Style"") DO UPDATE
                                        SET ""TimerTicks"" = CASE WHEN @TimerTicks < ""TimerTicks"" THEN @TimerTicks ELSE ""TimerTicks"" END,
                                        ""FormattedTime"" = CASE WHEN @TimerTicks < ""TimerTicks"" THEN @FormattedTime ELSE ""FormattedTime"" END";
                                    insertOrUpdateCommand = new NpgsqlCommand(insertOrUpdateQuery, (NpgsqlConnection)connection);
                                    break;
                                default:
                                    insertOrUpdateQuery = null;
                                    break;
                            }

                            using (insertOrUpdateCommand)
                            {
                                insertOrUpdateCommand!.AddParameterWithValue("@SteamID", steamId);
                                insertOrUpdateCommand!.AddParameterWithValue("@PlayerName", playerRecord.PlayerName!);
                                insertOrUpdateCommand!.AddParameterWithValue("@TimerTicks", playerRecord.TimerTicks);
                                insertOrUpdateCommand!.AddParameterWithValue("@FormattedTime", FormatTime(playerRecord.TimerTicks));
                                insertOrUpdateCommand!.AddParameterWithValue("@MapName", mapName);
                                insertOrUpdateCommand!.AddParameterWithValue("@UnixStamp", 0);
                                insertOrUpdateCommand!.AddParameterWithValue("@TimesFinished", 0);
                                insertOrUpdateCommand!.AddParameterWithValue("@LastFinished", 0);
                                insertOrUpdateCommand!.AddParameterWithValue("@Style", 0);

                                await insertOrUpdateCommand!.ExecuteNonQueryAsync();
                            }
                        }

                        SharpTimerDebug($"JSON times from {Path.GetFileName(filePath)} successfully added to the database.");
                    }
                }
            }
            catch (Exception ex)
            {
                SharpTimerError($"Error adding JSON times to the database: {ex.Message}");
            }
        }
    }
}