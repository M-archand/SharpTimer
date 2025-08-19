using CounterStrikeSharp.API.Core;
using System.Numerics;

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
                    case 0:  SetNormalStyle(player);    return;
                    case 1:  SetLowGravity(player);     return;
                    case 2:  SetSideways(player);       return;
                    case 3:  SetOnlyW(player);          return;
                    case 4:  Set400Vel(player);         return;
                    case 5:  SetHighGravity(player);    return;
                    case 6:  SetOnlyA(player);          return;
                    case 7:  SetOnlyD(player);          return;
                    case 8:  SetOnlyS(player);          return;
                    case 9:  SetHalfSideways(player);   return;
                    case 10: SetFastForward(player);    return;
                    case 11: SetParachute(player);      return;
                    case 12: SetTAS(player);            return;
                    default: return;
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

        // Preferred overload: pass numerics Vector3 (no native allocations)
        public void SetVelocity(CCSPlayerController player, Vector3 currentVel, int desiredVel)
        {
            // Write directly to engine fields
            var pawn = player!.PlayerPawn.Value!;
            if (currentVel.X >  desiredVel) pawn.AbsVelocity.X =  desiredVel;
            if (currentVel.X < -desiredVel) pawn.AbsVelocity.X = -desiredVel;
            if (currentVel.Y >  desiredVel) pawn.AbsVelocity.Y =  desiredVel;
            if (currentVel.Y < -desiredVel) pawn.AbsVelocity.Y = -desiredVel;
            // Do not cap Z velocity
        }

        // Backwards-compatible shim: if any call sites still pass the engine Vector, this funnels into the numerics version
        public void SetVelocity(CCSPlayerController player, CounterStrikeSharp.API.Modules.Utils.Vector currentVel, int desiredVel)
            => SetVelocity(player, (Vector3)currentVel, desiredVel);

        public void IncreaseVelocity(CCSPlayerController player)
        {
            // Convert engine velocity to numerics and compute 2D speed without using engine helpers
            Vector3 vel = (Vector3)player!.PlayerPawn.Value!.AbsVelocity;
            var currentSpeedXY = Math.Round(Math.Sqrt(vel.X * vel.X + vel.Y * vel.Y));
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
                0 => 1,
                1 => lowgravPointModifier,
                2 => sidewaysPointModifier,
                3 => onlywPointModifier,
                4 => velPointModifier,
                5 => highgravPointModifier,
                6 => onlyaPointModifier,
                7 => onlydPointModifier,
                8 => onlysPointModifier,
                9 => halfSidewaysPointModifier,
                10 => fastForwardPointModifier,
                11 => parachutePointModifier,
                12 => tasPointModifier,
                _ => 1,
            };
        }
    }
}