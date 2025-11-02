import { mysqlTable, int, text, timestamp } from 'drizzle-orm/mysql-core'

export const todos = mysqlTable('todos', {
  id: int('id').primaryKey().autoincrement(),
  title: text('title').notNull(),
  createdAt: timestamp('created_at', { mode: 'date' }).defaultNow(),
})
