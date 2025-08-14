ALTER TABLE "PlayerStageTimes" ADD COLUMN "Style" INT DEFAULT 0;
ALTER TABLE "PlayerStageTimes" ADD COLUMN "Mode" VARCHAR(24) DEFAULT '';
ALTER TABLE "PlayerStageTimes" DROP CONSTRAINT IF EXISTS "PlayerStageTimes_pkey";
ALTER TABLE "PlayerStageTimes" DROP CONSTRAINT IF EXISTS "pk_Stage";
ALTER TABLE "PlayerStageTimes" ADD CONSTRAINT pk_Stage PRIMARY KEY ("MapName", "SteamID", "Stage", "Style", "Mode");