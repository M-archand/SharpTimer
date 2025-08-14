using CounterStrikeSharp.API;
using CounterStrikeSharp.API.Core;
using CounterStrikeSharp.API.Core.Attributes.Registration;
using CounterStrikeSharp.API.Modules.Commands;
using CounterStrikeSharp.API.Modules.Cvars;
using CounterStrikeSharp.API.Modules.Memory.DynamicFunctions;

namespace SharpTimer;

public partial class SharpTimer
{
    private readonly ConVar? _wishspeed = ConVar.Find("sv_air_max_wishspeed");
    private readonly ConVar? _airaccel = ConVar.Find("sv_airaccelerate");
    private readonly ConVar? _accel = ConVar.Find("sv_accelerate");
    private readonly ConVar? _friction = ConVar.Find("sv_friction");

    private readonly Mode[] _playerModes = new Mode[64];

    public readonly struct ModeConfig
    {
        public readonly Mode Mode;
        public readonly float AirAccelerate;
        public readonly float Accelerate;
        public readonly float Wishspeed;
        public readonly float Friction;

        public ModeConfig(Mode mode, float airAccelerate, float accelerate, float wishspeed, float friction)
        {
            Mode = mode;
            AirAccelerate = airAccelerate;
            Accelerate = accelerate;
            Wishspeed = wishspeed;
            Friction = friction;
        }
    }

    private ModeConfig[] _configValues;

    public void InitializeModeConfigs()
    {
        _configValues = new ModeConfig[]
        {
            new(Mode.Standard, 150f, 10f, 30.0f, 5.2f),
            new(Mode._85t, 150f, 10f, 37.41f, 5.2f),
            new(Mode.Source, 150f, 5f, 30.71f, 4f),
            new(Mode.Arcade, 1000f, 10f, 43.55f, 4f),
            new(Mode._128t, 150f, 10f, 52.59f, 5.2f),
            new(Mode.Custom, customAirAccel, customAccel, customWishSpeed, customFriction)
        };
    }

    private readonly Dictionary<Mode, int> ModeIndexLookup = new()
    {
        { Mode.Standard, 0 },
        { Mode._85t, 1 },
        { Mode.Source, 2 },
        { Mode.Arcade, 3 },
        { Mode._128t, 4 },
        { Mode.Custom, 5 }
    };

    private bool TryParseMode(string input, out Mode mode)
    {
        if (Enum.TryParse<Mode>(input, true, out mode))
        {
            return true;
        }

        var modes = Enum.GetValues<Mode>();
        foreach (var m in modes)
        {
            if (GetModeName(m).Equals(input, StringComparison.OrdinalIgnoreCase))
            {
                mode = m;
                return true;
            }
        }

        mode = default;
        return false;
    }

    public void SetPlayerMode(CCSPlayerController player, Mode mode)
    {
        _playerModes[player.Slot] = mode;
        playerTimers[player.Slot].Mode = GetModeName(mode);
        playerTimers[player.Slot].ChangedMode = true;
        Server.NextFrame(async () =>
        {
            await SetPlayerStats(player, player.SteamID.ToString(), player.PlayerName, player.Slot);
        });
        ApplyModeSettings(player, mode);
    }

    private void ApplyModeSettings(CCSPlayerController player, Mode mode)
    {
        var config = _configValues[ModeIndexLookup[mode]];

        player.ReplicateConVar("sv_airaccelerate", config.AirAccelerate.ToString());
        player.ReplicateConVar("sv_accelerate", config.Accelerate.ToString());
        player.ReplicateConVar("sv_air_max_wishspeed", config.Wishspeed.ToString());
        player.ReplicateConVar("sv_friction", config.Friction.ToString());
    }

    private string GetModeName(Mode mode)
    {
        return mode switch
        {
            Mode.Standard => "Standard",
            Mode._85t => "85t",
            Mode.Source => "Source",
            Mode.Arcade => "Arcade",
            Mode._128t => "128t",
            Mode.Custom => "Custom",
            _ => mode.ToString()
        };
    }

    public double GetModeMultiplier(string mode, bool global = false)
    {
        if (global)
        {
            switch (mode.ToLower())
            {
                case "source":
                    return 1.1;
                case "standard":
                    return 1;
                case "85t":
                    return 0.9;
                case "arcade":
                    return 0.8;
                case "128t":
                    return 0.8;
                default:
                    return 1;
            }
        }

        switch (mode.ToLower())
        {
            case "source":
                return sourceModeModifier;
            case "standard":
                return standardModeModifier;
            case "85t":
                return _85tModeModifier;
            case "arcade":
                return arcadeModeModifier;
            case "128t":
                return _128tModeModifier;
            case "custom":
                return 1;
            default:
                return 1;
        }
    }

    [ConsoleCommand("css_mode")]
    public void ModeCommand(CCSPlayerController? player, CommandInfo command)
    {
        if (!IsAllowedPlayer(player))
            return;

        if (!player!.PlayerPawn.Value!.AbsVelocity.IsZero())
        {
            Utils.PrintToChat(player, Localizer["modes_moving"]);
            return;
        }

        if (CommandCooldown(player))
            return;

        var desiredMode = command.GetArg(1);

        playerTimers[player.Slot].IsTimerRunning = false;
        playerTimers[player.Slot].TimerTicks = 0;
        playerTimers[player.Slot].IsBonusTimerRunning = false;
        playerTimers[player.Slot].BonusTimerTicks = 0;

        if (command.ArgByIndex(1) == "")
        {
            var modes = Enum.GetValues<Mode>().ToArray();

            for (int i = 0; i < modes.Length; i++)
            {
                Utils.PrintToChat(player, Localizer["modes_list", i, GetModeName(modes[i])]);
            }

            Utils.PrintToChat(player, Localizer["mode_example"]);
            return;
        }

        if (int.TryParse(desiredMode, out int modeIndex))
        {
            var modes = Enum.GetValues<Mode>().ToArray();

            if (modeIndex >= 0 && modeIndex < modes.Length)
            {
                Mode newMode = modes[modeIndex];
                SetPlayerMode(player, newMode);
                Utils.PrintToChat(player, Localizer["mode_set", GetModeName(newMode)]);
                RespawnPlayer(player);
            }
            else
            {
                Utils.PrintToChat(player, Localizer["mode_not_found", desiredMode]);
            }
        }
        else if (TryParseMode(desiredMode.ToLower(), out Mode newMode))
        {
            SetPlayerMode(player, newMode);
            Utils.PrintToChat(player, Localizer["mode_set", GetModeName(newMode)]);
            RespawnPlayer(player);
        }
        else
        {
            Utils.PrintToChat(player, Localizer["mode_not_found", desiredMode]);
        }
    }

    public int? GetSlot(CCSPlayer_MovementServices? movementServices)
    {
        uint? index = movementServices?.Pawn.Value?.Controller.Value?.Index;
        if (index == null)
        {
            return null;
        }

        return (int)index.Value - 1;
    }

    public Mode? GetPlayerMode(CCSPlayer_MovementServices movementServices)
    {
        int? slot = GetSlot(movementServices);
        if (slot == null)
        {
            return null;
        }

        return _playerModes[slot.Value];
    }

    private readonly Dictionary<string, bool> _wasConVarChanged = new();

    private HookResult ApplyConvar(DynamicHook hook)
    {
        CCSPlayerController player = hook.GetParam<CCSPlayer_MovementServices>(0).Pawn.Value.Controller.Value?.As<CCSPlayerController>();
        CCSPlayer_MovementServices movementServices = hook.GetParam<CCSPlayer_MovementServices>(0);
        Mode? playerMode = GetPlayerMode(movementServices);
        
        if (_accel == null || _airaccel == null || _wishspeed == null || _friction == null || player == null)
        {
            return HookResult.Continue;
        }

        _wasConVarChanged["sv_accelerate"] = false;
        _wasConVarChanged["sv_airaccelerate"] = false;
        _wasConVarChanged["sv_air_max_wishspeed"] = false;
        _wasConVarChanged["sv_friction"] = false;

        if (playerMode == null || !ModeIndexLookup.ContainsKey(playerMode.Value))
        {
            return HookResult.Continue;
        }

        ModeConfig modeConfig = _configValues[ModeIndexLookup[playerMode.Value]];

        if (!_accel.GetPrimitiveValue<float>().ToString().Equals(modeConfig.Accelerate))
        {
            _accel.SetValue(modeConfig.Accelerate);
            _wasConVarChanged["sv_accelerate"] = true;
        }

        if (!_airaccel.GetPrimitiveValue<float>().ToString().Equals(modeConfig.AirAccelerate))
        {
            _airaccel.SetValue(modeConfig.AirAccelerate);
            _wasConVarChanged["sv_airaccelerate"] = true;
        }

        if (!_wishspeed.GetPrimitiveValue<float>().ToString().Equals(modeConfig.Wishspeed))
        {
            _wishspeed.SetValue(modeConfig.Wishspeed);
            _wasConVarChanged["sv_air_max_wishspeed"] = true;
        }

        if (!_friction.GetPrimitiveValue<float>().ToString().Equals(modeConfig.Friction))
        {
            _friction.SetValue(modeConfig.Friction);
            _wasConVarChanged["sv_friction"] = true;
        }

        return HookResult.Continue;
    }

    private HookResult ResetConvar(DynamicHook hook)
    {
        CCSPlayer_MovementServices movementServices = hook.GetParam<CCSPlayer_MovementServices>(0);
        CCSPlayerController player = hook.GetParam<CCSPlayer_MovementServices>(0).Pawn.Value.Controller.Value?.As<CCSPlayerController>();
        Mode? playerMode = GetPlayerMode(movementServices);

        if (_accel == null || _airaccel == null || _wishspeed == null || _friction == null || player == null)
        {
            return HookResult.Continue;
        }

        var defaultConfig = _configValues[ModeIndexLookup[defaultMode]];

        if (_wasConVarChanged.TryGetValue("sv_accelerate", out bool accelChanged) && accelChanged)
        {
            _accel.SetValue(defaultConfig.Accelerate);
        }

        if (_wasConVarChanged.TryGetValue("sv_airaccelerate", out bool airAccelChanged) && airAccelChanged)
        {
            _airaccel.SetValue(defaultConfig.AirAccelerate);
        }

        if (_wasConVarChanged.TryGetValue("sv_air_max_wishspeed", out bool wishspeedChanged) && wishspeedChanged)
        {
            _wishspeed.SetValue(defaultConfig.Wishspeed);
        }

        if (_wasConVarChanged.TryGetValue("sv_friction", out bool frictionChanged) && frictionChanged)
        {
            _friction.SetValue(defaultConfig.Friction);
        }

        return HookResult.Continue;
    }
}

[Flags]
public enum Mode
{
    Standard = 0,  // default csgo 1:1 cfg (64 tick)
    _85t = 1,      // 85t-ish speed (37.41 wishspeed)
    Source = 2,    // 66t-ish + lower accel + cs:s friction (30.71 wishspeed & 5 accel & 4 friction)
    Arcade = 3,    // 102.4t-ish + higher aa + cs:s friction (43.55 wishspeed & 1000 aa & 4 friction)
    _128t = 4,     // 128t-ish speed (52.59 wishspeed)
    Custom = 5     // Use custom server cvars (WILL NOT SUBMIT TO GLOBAL)
}