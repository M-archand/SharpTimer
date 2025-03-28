using CounterStrikeSharp.API.Core;
using Vector = CounterStrikeSharp.API.Modules.Utils.Vector;

namespace SharpTimer
{
    public partial class SharpTimer
    {
        public void setStyle(CCSPlayerController player, int style)
        {
            AddTimer(0.1f, () =>
            {
                SetNormalStyle(player);
                switch (style)
                {
                    case 0:
                        SetNormalStyle(player);
                        return;
                    case 1:
                        SetLowGravity(player);
                        return;
                    case 2:
                        SetSideways(player);
                        return;
                    case 3:
                        SetOnlyW(player);
                        return;
                    case 4:
                        Set400Vel(player);
                        return;
                    case 5:
                        SetHighGravity(player);
                        return;
                    case 6:
                        SetOnlyA(player);
                        return;
                    case 7:
                        SetOnlyD(player);
                        return;
                    case 8:
                        SetOnlyS(player);
                        return;
                    case 9:
                        SetHalfSideways(player);
                        return;
                    case 10:
                        SetFastForward(player);
                        return;
                    case 11:
                        SetParachute(player);
                        return;
                    case 12:
                        SetTAS(player);
                        return;
                    default:
                        return;
                }
            });
        }

        public void SetNormalStyle(CCSPlayerController player)
        {
            playerTimers[player.Slot].currentStyle = 0; // reset currentStyle
            playerTimers[player.Slot].changedStyle = true;
            player!.Pawn.Value!.GravityScale = 1f;
        }

        public void SetLowGravity(CCSPlayerController player)
        {
            playerTimers[player.Slot].currentStyle = 1; // 1 = low-gravity
            player!.Pawn.Value!.GravityScale = 0.5f;
            playerTimers[player.Slot].changedStyle = true;
        }
        public void SetHighGravity(CCSPlayerController player)
        {
            playerTimers[player.Slot].currentStyle = 5; // 5 = high-gravity
            player!.Pawn.Value!.GravityScale = 1.5f;
            playerTimers[player.Slot].changedStyle = true;
        }
        public void SetSlowMo(CCSPlayerController player)
        {
            //playerTimers[player.Slot].currentStyle = ?; // ? = slowmo (its broken)
            //Schema.SetSchemaValue(player!.Pawn.Value!.Handle, "CBaseEntity", "m_flTimeScale", 0.5f);
            //Utilities.SetStateChanged(player!.Pawn.Value!, "CBaseEntity", "m_flTimeScale");
        }

        public void SetSideways(CCSPlayerController player)
        {
            playerTimers[player.Slot].currentStyle = 2; // 2 = sideways
            playerTimers[player.Slot].changedStyle = true;
        }
        public void SetHalfSideways(CCSPlayerController player)
        {
            playerTimers[player.Slot].currentStyle = 9; // 9 = halfsideways
            playerTimers[player.Slot].changedStyle = true;
        }
        public void SetFastForward(CCSPlayerController player)
        {
            playerTimers[player.Slot].currentStyle = 10; // 10 = fastforward
            playerTimers[player.Slot].changedStyle = true;
        }

        public void SetOnlyW(CCSPlayerController player)
        {
            playerTimers[player.Slot].currentStyle = 3; // 3 = only w
            playerTimers[player.Slot].changedStyle = true;
        }
        public void SetOnlyA(CCSPlayerController player)
        {
            playerTimers[player.Slot].currentStyle = 6; // 6 = only a
            playerTimers[player.Slot].changedStyle = true;
        }
        public void SetOnlyD(CCSPlayerController player)
        {
            playerTimers[player.Slot].currentStyle = 7; // 7 = only d
            playerTimers[player.Slot].changedStyle = true;
        }
        public void SetOnlyS(CCSPlayerController player)
        {
            playerTimers[player.Slot].currentStyle = 8; // 8 = only s
            playerTimers[player.Slot].changedStyle = true;
        }

        public void Set400Vel(CCSPlayerController player)
        {
            playerTimers[player.Slot].currentStyle = 4; // 4 = 400vel
            playerTimers[player.Slot].changedStyle = true;
        }
        
        public void SetParachute(CCSPlayerController player)
        {
            playerTimers[player.Slot].currentStyle = 11; // 11 = parachute
            playerTimers[player.Slot].changedStyle = true;
        }
        
        public void SetTAS(CCSPlayerController player)
        {
            playerTimers[player.Slot].currentStyle = 12; // 12 = TAS
            playerTimers[player.Slot].changedStyle = true;
        }

        public void SetVelocity(CCSPlayerController player, Vector currentVel, int desiredVel)
        {
            if (currentVel.X > desiredVel) player!.PlayerPawn.Value!.AbsVelocity.X = desiredVel;
            if (currentVel.X < -desiredVel) player!.PlayerPawn.Value!.AbsVelocity.X = -desiredVel;
            if (currentVel.Y > desiredVel) player!.PlayerPawn.Value!.AbsVelocity.Y = desiredVel;
            if (currentVel.Y < -desiredVel) player!.PlayerPawn.Value!.AbsVelocity.Y = -desiredVel;
            //do not cap z velocity
        }

        public void IncreaseVelocity(CCSPlayerController player)
        {
            var currentSpeedXY = Math.Round(player!.Pawn.Value!.AbsVelocity.Length2D());
            var targetSpeed = currentSpeedXY + 5;

            AdjustPlayerVelocity2D(player, (float)targetSpeed);
        }

        public string GetNamedStyle(int style)
        {
            return style switch
            {
                0 => "Normal",
                1 => "Low Gravity",
                2 => "Sideways",
                3 => "OnlyW",
                4 => "400vel",
                5 => "High Gravity",
                6 => "OnlyA",
                7 => "OnlyD",
                8 => "OnlyS",
                9 => "Half Sideways",
                10 => "Fast Forward",
                11 => "Parachute",
                12 => "TAS",
                _ => "null",
            };
        }

        public double GetStyleMultiplier(int style, bool global = false)
        {
            if (global)
            {
                return style switch
                {
                    0 => 1,
                    1 => 0.8,
                    2 => 1.3,
                    3 => 1.3,
                    4 => 1.5,
                    5 => 1,
                    6 => 1.33,
                    7 => 1.33,
                    8 => 1.33,
                    9 => 1.3,
                    10 => 0.8,
                    11 => 0.8,
                    12 => 0.0,
                    _ => 1,
                };
            }
            return style switch
            {
                0 => 1, // 1.0x for normal
                1 => lowgravPointModifier, //1.1x for lowgrav
                2 => sidewaysPointModifier, // 1.3x for sideways
                3 => onlywPointModifier, // 1.33x for onlyw
                4 => velPointModifier, // 1.5x for 400vel
                5 => highgravPointModifier, // 1.3x for highgrav
                6 => onlyaPointModifier, // 1.33x for onlya
                7 => onlydPointModifier, // 1.33x for onlyd
                8 => onlysPointModifier, // 1.33x for onlys
                9 => halfSidewaysPointModifier, // 1.3x for halfsideways
                10 => fastForwardPointModifier, // 1.3x for ff
                11 => parachutePointModifier, // 0.8x for parachute
                12 => tasPointModifier, // 0.0x for TAS
                _ => 1,
            };
        }
    }
}