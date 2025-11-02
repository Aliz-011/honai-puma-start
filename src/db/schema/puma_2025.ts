import { mysqlSchema, varchar, decimal, index, int } from "drizzle-orm/mysql-core";
import { relations } from "drizzle-orm";

export const pumaSchema = mysqlSchema("puma_2025");

export const regionals = pumaSchema.table("regionals", {
    id: varchar("id", { length: 100 }).primaryKey(),
    regional: varchar("regional", { length: 40 }).notNull(),
});

export const regionalRelations = relations(regionals, ({ many }) => ({
    branches: many(branches),
}));

export const branches = pumaSchema.table("branches_new", {
    id: varchar("id", { length: 100 }).primaryKey(),
    regionalId: varchar("id_regional", { length: 100 }).notNull(),
    branchNew: varchar("branch_new", { length: 30 }).notNull(),
}, t => [
    index('idx_regional_id').on(t.regionalId).using('btree')
]);

export const branchRelations = relations(branches, ({ one, many }) => ({
    regional: one(regionals, {
        fields: [branches.regionalId],
        references: [regionals.id],
    }),
    subbranches: many(subbranches),
    woks: many(woks)
}));

export const subbranches = pumaSchema.table("subbranches_new", {
    id: varchar("id", { length: 100 }).primaryKey(),
    branchId: varchar("id_branch", { length: 100 }).notNull(),
    subbranchNew: varchar("subbranch_new", { length: 30 }).notNull(),
}, t => [
    index('idx_branch_id').on(t.branchId).using('btree')
]);

export const subbranchRelations = relations(subbranches, ({ one, many }) => ({
    branch: one(branches, {
        fields: [subbranches.branchId],
        references: [branches.id],
    }),
    clusters: many(clusters),
}));

export const clusters = pumaSchema.table("clusters_new", {
    id: varchar("id", { length: 100 }).primaryKey(),
    subbranchId: varchar("subbranch_id", { length: 100 }).notNull(),
    cluster: varchar("cluster", { length: 30 }).notNull(),
}, t => [
    index('idx_subbranch_id').on(t.subbranchId).using('btree')
]);

export const clusterRelations = relations(clusters, ({ one, many }) => ({
    subbranch: one(subbranches, {
        fields: [clusters.subbranchId],
        references: [subbranches.id],
    }),
    kabupatens: many(kabupatens),
}));

export const kabupatens = pumaSchema.table("kabupatens", {
    id: varchar({ length: 100 }).primaryKey(),
    clusterId: varchar("id_cluster", { length: 100 }).notNull(),
    kabupaten: varchar({ length: 30 }).notNull(),
}, t => [
    index('idx_cluster_id').on(t.clusterId).using('btree')
]);

export const kabupatenRelations = relations(kabupatens, ({ one, many }) => ({
    cluster: one(clusters, {
        fields: [kabupatens.clusterId],
        references: [clusters.id],
    }),

}));

export const territoryArea4 = pumaSchema.table('teritory_area4_2023', {
    regional: varchar('regional', { length: 100 }),
    branch: varchar('branch', { length: 100 }),
    subbranch: varchar('subbranch', { length: 100 }),
    cluster: varchar('cluster', { length: 100 }),
    kabupaten: varchar('kabupaten', { length: 100 }),
}, t => [
    index('teritory_area4_2023_regional_IDX').on(t.regional, t.branch, t.subbranch, t.cluster, t.kabupaten).using('btree')
])

export const territoryHousehold = pumaSchema.table('ref_teritory_household', {
    area: varchar('area', { length: 20 }),
    regional: varchar('regional', { length: 100 }),
    branch: varchar('branch', { length: 100 }),
    cluster: varchar('cluster', { length: 100 }),
    kabupaten: varchar('kabupaten', { length: 200 }),
    wok: varchar('wok', { length: 100 }),
    sto: varchar('sto', { length: 100 }),
    nama_sto: varchar('nama_sto', { length: 100 }),
    code_nama_sto: varchar('code_nama_sto', { length: 100 }),
})

export const territoryArea4RgbHq = pumaSchema.table('teritory_area4_2023_rgb_hq', {
    regional: varchar('regional', { length: 50 }),
    branch: varchar('branch', { length: 50 }),
    subbranch: varchar('subbranch', { length: 50 }),
    cluster: varchar('cluster', { length: 50 }),
    kabupaten: varchar('kabupaten', { length: 50 }),
}, t => ({
    teritory_area4_2023_kabupaten_IDX: index('teritory_area4_2023_kabupaten_IDX').on(t.kabupaten).using('btree')
}))

export const woks = pumaSchema.table('wok', {
    id: int().primaryKey(),
    wok: varchar({ length: 30 }).notNull(),
    branchId: varchar('branch_id', { length: 100 })
})

export const wokRelations = relations(woks, ({ one, many }) => ({
    branch: one(branches, {
        fields: [woks.branchId],
        references: [branches.id]
    }),
    stos: many(stos)
}))

export const stos = pumaSchema.table('sto', {
    id: int().primaryKey(),
    sto: varchar({ length: 4 }).notNull(),
    wokId: int('wok_id').notNull()
})

export const stoRelations = relations(stos, ({ one }) => ({
    wok: one(woks, {
        fields: [stos.wokId],
        references: [woks.id]
    })
}))

export const all_territory_puma_2024 = pumaSchema.table('all_territory_puma_2024', {
    branch: varchar({ length: 30 }),
    subbranch: varchar({ length: 30 }),
    cluster: varchar({ length: 30 }),
    kabupaten: varchar({ length: 30 }),
    kecamatan: varchar({ length: 40 })
})

export const revenueSA = pumaSchema.table("Target_revenue_sa", {
    id: varchar("id", { length: 100 }).primaryKey(),
    kabupatenId: varchar("id_kabupaten", { length: 100 }).notNull(),
    m1: decimal("m1", { precision: 18, scale: 7 }),
    m2: decimal("m2", { precision: 18, scale: 7 }),
    m3: decimal("m3", { precision: 18, scale: 7 }),
    m4: decimal("m4", { precision: 18, scale: 7 }),
    m5: decimal("m5", { precision: 18, scale: 7 }),
    m6: decimal("m6", { precision: 18, scale: 7 }),
    m7: decimal("m7", { precision: 18, scale: 7 }),
    m8: decimal("m8", { precision: 18, scale: 7 }),
    m9: decimal("m9", { precision: 18, scale: 7 }),
    m10: decimal("m10", { precision: 18, scale: 7 }),
    m11: decimal("m11", { precision: 18, scale: 7 }),
    m12: decimal("m12", { precision: 18, scale: 7 }),
    year: varchar({ length: 5 }).notNull(),
});

export const revenueSARelations = relations(revenueSA, ({ one }) => ({
    kabupaten: one(kabupatens, {
        fields: [revenueSA.kabupatenId],
        references: [kabupatens.id],
    }),
}));

export const revenueSAPrabayar = pumaSchema.table("Target_revenue_sa_prabayar", {
    id: varchar("id", { length: 100 }).primaryKey(),
    kabupatenId: varchar("id_kabupaten", { length: 100 }).notNull(),
    m1: decimal("m1", { precision: 18, scale: 7 }),
    m2: decimal("m2", { precision: 18, scale: 7 }),
    m3: decimal("m3", { precision: 18, scale: 7 }),
    m4: decimal("m4", { precision: 18, scale: 7 }),
    m5: decimal("m5", { precision: 18, scale: 7 }),
    m6: decimal("m6", { precision: 18, scale: 7 }),
    m7: decimal("m7", { precision: 18, scale: 7 }),
    m8: decimal("m8", { precision: 18, scale: 7 }),
    m9: decimal("m9", { precision: 18, scale: 7 }),
    m10: decimal("m10", { precision: 18, scale: 7 }),
    m11: decimal("m11", { precision: 18, scale: 7 }),
    m12: decimal("m12", { precision: 18, scale: 7 }),
    year: varchar({ length: 5 }).notNull(),
});

export const revenueSAPrabayarRelations = relations(revenueSAPrabayar, ({ one }) => ({
    kabupaten: one(kabupatens, {
        fields: [revenueSAPrabayar.kabupatenId],
        references: [kabupatens.id],
    }),
}));

export const revenueSAByu = pumaSchema.table("Target_revenue_sa_byu", {
    id: varchar("id", { length: 100 }).primaryKey(),
    kabupatenId: varchar("id_kabupaten", { length: 100 }).notNull(),
    m1: decimal("m1", { precision: 18, scale: 7 }),
    m2: decimal("m2", { precision: 18, scale: 7 }),
    m3: decimal("m3", { precision: 18, scale: 7 }),
    m4: decimal("m4", { precision: 18, scale: 7 }),
    m5: decimal("m5", { precision: 18, scale: 7 }),
    m6: decimal("m6", { precision: 18, scale: 7 }),
    m7: decimal("m7", { precision: 18, scale: 7 }),
    m8: decimal("m8", { precision: 18, scale: 7 }),
    m9: decimal("m9", { precision: 18, scale: 7 }),
    m10: decimal("m10", { precision: 18, scale: 7 }),
    m11: decimal("m11", { precision: 18, scale: 7 }),
    m12: decimal("m12", { precision: 18, scale: 7 }),
    year: varchar({ length: 5 }).notNull(),
});

export const revenueSAByuRelations = relations(revenueSAByu, ({ one }) => ({
    kabupaten: one(kabupatens, {
        fields: [revenueSAByu.kabupatenId],
        references: [kabupatens.id],
    }),
}));

export const trxSA = pumaSchema.table("Target_trx_sa", {
    id: varchar("id", { length: 100 }).primaryKey(),
    kabupatenId: varchar("id_kabupaten", { length: 100 }).notNull(),
    m1: decimal("m1", { precision: 18, scale: 7 }),
    m2: decimal("m2", { precision: 18, scale: 7 }),
    m3: decimal("m3", { precision: 18, scale: 7 }),
    m4: decimal("m4", { precision: 18, scale: 7 }),
    m5: decimal("m5", { precision: 18, scale: 7 }),
    m6: decimal("m6", { precision: 18, scale: 7 }),
    m7: decimal("m7", { precision: 18, scale: 7 }),
    m8: decimal("m8", { precision: 18, scale: 7 }),
    m9: decimal("m9", { precision: 18, scale: 7 }),
    m10: decimal("m10", { precision: 18, scale: 7 }),
    m11: decimal("m11", { precision: 18, scale: 7 }),
    m12: decimal("m12", { precision: 18, scale: 7 }),
    year: varchar({ length: 5 }).notNull(),
});

export const trxSARelations = relations(trxSA, ({ one }) => ({
    kabupaten: one(kabupatens, {
        fields: [trxSA.kabupatenId],
        references: [kabupatens.id],
    }),
}));

export const trxSAPrabayar = pumaSchema.table("Target_trx_sa_prabayar", {
    id: varchar("id", { length: 100 }).primaryKey(),
    kabupatenId: varchar("id_kabupaten", { length: 100 }).notNull(),
    m1: decimal("m1", { precision: 18, scale: 7 }),
    m2: decimal("m2", { precision: 18, scale: 7 }),
    m3: decimal("m3", { precision: 18, scale: 7 }),
    m4: decimal("m4", { precision: 18, scale: 7 }),
    m5: decimal("m5", { precision: 18, scale: 7 }),
    m6: decimal("m6", { precision: 18, scale: 7 }),
    m7: decimal("m7", { precision: 18, scale: 7 }),
    m8: decimal("m8", { precision: 18, scale: 7 }),
    m9: decimal("m9", { precision: 18, scale: 7 }),
    m10: decimal("m10", { precision: 18, scale: 7 }),
    m11: decimal("m11", { precision: 18, scale: 7 }),
    m12: decimal("m12", { precision: 18, scale: 7 }),
    year: varchar({ length: 5 }).notNull(),
});

export const trxSAPrabayarRelations = relations(trxSAPrabayar, ({ one }) => ({
    kabupaten: one(kabupatens, {
        fields: [trxSAPrabayar.kabupatenId],
        references: [kabupatens.id],
    }),
}));

export const trxSAByu = pumaSchema.table("Target_trx_sa_byu", {
    id: varchar("id", { length: 100 }).primaryKey(),
    kabupatenId: varchar("id_kabupaten", { length: 100 }).notNull(),
    m1: decimal("m1", { precision: 18, scale: 7 }),
    m2: decimal("m2", { precision: 18, scale: 7 }),
    m3: decimal("m3", { precision: 18, scale: 7 }),
    m4: decimal("m4", { precision: 18, scale: 7 }),
    m5: decimal("m5", { precision: 18, scale: 7 }),
    m6: decimal("m6", { precision: 18, scale: 7 }),
    m7: decimal("m7", { precision: 18, scale: 7 }),
    m8: decimal("m8", { precision: 18, scale: 7 }),
    m9: decimal("m9", { precision: 18, scale: 7 }),
    m10: decimal("m10", { precision: 18, scale: 7 }),
    m11: decimal("m11", { precision: 18, scale: 7 }),
    m12: decimal("m12", { precision: 18, scale: 7 }),
    year: varchar({ length: 5 }).notNull(),
});

export const trxSAByuRelations = relations(trxSAByu, ({ one }) => ({
    kabupaten: one(kabupatens, {
        fields: [trxSAByu.kabupatenId],
        references: [kabupatens.id],
    }),
}));

export const targetSO = pumaSchema.table("Target_so", {
    id: varchar("id", { length: 100 }).primaryKey(),
    kabupatenId: varchar("id_kabupaten", { length: 100 }).notNull(),
    m1: decimal("m1", { precision: 18, scale: 7 }),
    m2: decimal("m2", { precision: 18, scale: 7 }),
    m3: decimal("m3", { precision: 18, scale: 7 }),
    m4: decimal("m4", { precision: 18, scale: 7 }),
    m5: decimal("m5", { precision: 18, scale: 7 }),
    m6: decimal("m6", { precision: 18, scale: 7 }),
    m7: decimal("m7", { precision: 18, scale: 7 }),
    m8: decimal("m8", { precision: 18, scale: 7 }),
    m9: decimal("m9", { precision: 18, scale: 7 }),
    m10: decimal("m10", { precision: 18, scale: 7 }),
    m11: decimal("m11", { precision: 18, scale: 7 }),
    m12: decimal("m12", { precision: 18, scale: 7 }),
    year: varchar({ length: 5 }).notNull(),
});

export const targetSORelations = relations(targetSO, ({ one }) => ({
    kabupaten: one(kabupatens, {
        fields: [targetSO.kabupatenId],
        references: [kabupatens.id],
    }),
}));

export type Kabupaten = typeof kabupatens.$inferSelect
export type Cluster = typeof clusters.$inferSelect
export type Subbranch = typeof subbranches.$inferSelect
export type Branch = typeof branches.$inferSelect
export type Regional = typeof regionals.$inferSelect