-- 320_drop_pricing_config_free_openai_fast.sql removes the retired free_openai_fast
-- toggle from shared pricing configurations so user charges and public quotes
-- always use the effective service-tier rate.
ALTER TABLE pricing_configs
    DROP COLUMN IF EXISTS free_openai_fast;
