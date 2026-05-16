-- ====== 评分表修复 ======
-- 运行前建议先备份

-- 1. 添加 order_id 列
ALTER TABLE ratings ADD COLUMN order_id INT NOT NULL DEFAULT 0 AFTER dish_id;

-- 2. 添加 user_id 单独索引（因为将删除覆盖它的联合索引）
ALTER TABLE ratings ADD INDEX idx_user_id (user_id);

-- 3. 删除旧外键（依赖旧索引）
ALTER TABLE ratings DROP FOREIGN KEY ratings_ibfk_1;
ALTER TABLE ratings DROP FOREIGN KEY ratings_ibfk_2;

-- 4. 删除旧的 (user_id, dish_id) 唯一索引
ALTER TABLE ratings DROP INDEX uk_user_dish;

-- 5. 建立新的 (user_id, order_id, dish_id) 联合唯一索引
ALTER TABLE ratings ADD UNIQUE INDEX unique_user_order_dish (user_id, order_id, dish_id);

-- 6. 重建外键
ALTER TABLE ratings ADD CONSTRAINT ratings_ibfk_1 FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE;
ALTER TABLE ratings ADD CONSTRAINT ratings_ibfk_2 FOREIGN KEY (dish_id) REFERENCES dishes(id) ON DELETE CASCADE;
