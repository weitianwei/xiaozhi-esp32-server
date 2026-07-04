-- Add Tencent Cloud LKE/QBot agent as an LLM provider and model config.
SET @provider_exists = (SELECT COUNT(*) FROM ai_model_provider WHERE id = 'SYSTEM_LLM_TencentAgent');
SET @sql = IF(@provider_exists = 0,
    'INSERT INTO `ai_model_provider` (`id`, `model_type`, `provider_code`, `name`, `fields`, `sort`, `creator`, `create_date`, `updater`, `update_date`) VALUES (''SYSTEM_LLM_TencentAgent'', ''LLM'', ''TencentAgent'', ''腾讯云智能体'', ''[{"key":"websocket_url","label":"WebSocket地址","type":"string"},{"key":"token_url","label":"Token接口地址","type":"string"},{"key":"token","label":"固定Token","type":"string"},{"key":"agent_id","label":"智能体ID","type":"string"},{"key":"source","label":"来源标识","type":"string"},{"key":"decrypt_token","label":"是否解密Token","type":"boolean"},{"key":"encryption_key","label":"Token解密密钥","type":"string"},{"key":"timeout","label":"超时时间秒","type":"number"}]'', 15, 1, NOW(), 1, NOW())',
    'SELECT ''SYSTEM_LLM_TencentAgent already exists, skip'' AS msg');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;

SET @model_exists = (SELECT COUNT(*) FROM ai_model_config WHERE id = 'LLM_TencentAgent');
SET @sql = IF(@model_exists = 0,
    'INSERT INTO `ai_model_config` (`id`, `model_type`, `model_code`, `model_name`, `is_default`, `is_enabled`, `config_json`, `doc_link`, `remark`, `sort`, `creator`, `create_date`, `updater`, `update_date`) VALUES (''LLM_TencentAgent'', ''LLM'', ''TencentAgent'', ''腾讯云智能体'', 0, 1, ''{"type":"TencentAgent","websocket_url":"wss://wss.lke.cloud.tencent.com/v1/qbot/chat/conn/?EIO=4&transport=websocket","token_url":"","token":"","agent_id":"","source":"","decrypt_token":false,"encryption_key":"","timeout":120}'', NULL, ''通过腾讯云 LKE/QBot WebSocket 调用已训练好的智能体和知识库；请在智控台中填写 token 或 token_url 等参数'', 15, NULL, NULL, NULL, NULL)',
    'SELECT ''LLM_TencentAgent already exists, skip'' AS msg');
PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;
