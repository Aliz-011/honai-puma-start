import { mysqlSchema, mysqlTable, varchar, timestamp, boolean, index } from "drizzle-orm/mysql-core";
import { v7 as uuidv7 } from 'uuid'

const authSchema = mysqlSchema('db_auth')

export const users = authSchema.table("users", {
  id: varchar("id", { length: 36 }).primaryKey().$defaultFn(() => uuidv7()),
  name: varchar("name", { length: 50 }).notNull(),
  email: varchar("email", { length: 100 }).notNull().unique(),
  emailVerified: boolean("email_verified").default(false).notNull(),
  image: varchar("image", { length: 100 }),
  createdAt: timestamp("created_at").defaultNow().notNull(),
  updatedAt: timestamp("updated_at")
    .defaultNow()
    .$onUpdate(() => new Date())
    .notNull(),
  username: varchar("username", { length: 50 }).unique(),
  displayUsername: varchar("display_username", { length: 50 }),
});

export const sessions = authSchema.table("sessions", {
  id: varchar("id", { length: 36 }).primaryKey().$defaultFn(() => uuidv7()),
  expiresAt: timestamp("expires_at").notNull(),
  token: varchar("token", { length: 100 }).notNull().unique(),
  createdAt: timestamp("created_at").defaultNow().notNull(),
  updatedAt: timestamp("updated_at")
    .$onUpdate(() => new Date())
    .notNull(),
  ipAddress: varchar("ip_address", { length: 50 }),
  userAgent: varchar("user_agent", { length: 255 }),
  userId: varchar("user_id", { length: 36 })
    .notNull()
    .references(() => users.id, { onDelete: "cascade" }),
}, (table) => ({
  userIdIdx: index("sessions_user_id_idx").on(table.userId).using('btree'),
  expiresAtIdx: index("sessions_expires_at_idx").on(table.expiresAt).using('btree'),
}));

export const accounts = authSchema.table("accounts", {
  id: varchar("id", { length: 36 }).primaryKey().$defaultFn(() => uuidv7()),
  accountId: varchar("account_id", { length: 40 }).notNull(),
  providerId: varchar("provider_id", { length: 40 }).notNull(),
  userId: varchar("user_id", { length: 36 })
    .notNull()
    .references(() => users.id, { onDelete: "cascade" }),
  accessToken: varchar("access_token", { length: 100 }),
  refreshToken: varchar("refresh_token", { length: 100 }),
  idToken: varchar("id_token", { length: 100 }),
  accessTokenExpiresAt: timestamp("access_token_expires_at"),
  refreshTokenExpiresAt: timestamp("refresh_token_expires_at"),
  scope: varchar("scope", { length: 100 }),
  password: varchar("password", { length: 255 }),
  createdAt: timestamp("created_at").defaultNow().notNull(),
  updatedAt: timestamp("updated_at")
    .$onUpdate(() => new Date())
    .notNull(),
}, (table) => ({
  userIdIdx: index("accounts_user_id_idx").on(table.userId),
  accountProviderIdx: index("accounts_provider_idx").on(table.accountId, table.providerId),
}));

export const verifications = authSchema.table("verifications", {
  id: varchar("id", { length: 36 }).primaryKey().$defaultFn(() => uuidv7()),
  identifier: varchar("identifier", { length: 50 }).notNull(),
  value: varchar("value", { length: 50 }).notNull(),
  expiresAt: timestamp("expires_at").notNull(),
  createdAt: timestamp("created_at").defaultNow().notNull(),
  updatedAt: timestamp("updated_at")
    .defaultNow()
    .$onUpdate(() => new Date())
    .notNull(),
}, (table) => ({
  identifierIdx: index("verifications_identifier_idx").on(table.identifier),
}));