-- 011: 世界观扩展字段 JSON（单存储，替代 Bible.world_settings 双写）
ALTER TABLE worldbuilding ADD COLUMN extensions_json TEXT DEFAULT '{}';
