import { mysqlSchema, varchar, decimal, index, date } from "drizzle-orm/mysql-core";

export const digiposRevampSchema = mysqlSchema("digipos_revamp");

export const dynamicRevenueSATable = (year: string, month: string) => {
    return digiposRevampSchema.table(`sa_detil_${year}${month}`, {
        trxDate: date('trx_date', { mode: 'string' }),
        regional: varchar({ length: 100 }),
        branch: varchar({ length: 100 }),
        subbranch: varchar({ length: 100 }),
        cluster: varchar({ length: 100 }),
        kabupaten: varchar({ length: 100 }),
        outletId: varchar('outlet_id', { length: 100 }),
        noRs: varchar('no_rs', { length: 100 }),
        sbpType: varchar('sbp_type', { length: 100 }),
        saType: varchar('sa_type', { length: 100 }),
        packKeyword: varchar('pack_keyword', { length: 100 }),
        packageType: varchar('package_type', { length: 200 }),
        brand: varchar('brand', { length: 100 }),
        msisdn: varchar('msisdn', { length: 100 }),
        price: decimal('price', { precision: 38, scale: 2 }),
    }, t => [
        index('msisdn').on(t.msisdn).using('btree')
    ])
}

export const dynamicRevenueSOTable = (year: string, month: string) => {
    return digiposRevampSchema.table(`so_detil_${year}${month}`, {
        dateSo: varchar('date_so', { length: 15 }),
        dateSa: varchar('date_sa', { length: 15 }),
        regionSa: varchar('region_sa', { length: 100 }),
        citySa: varchar('city_sa', { length: 100 }),
        regionSo: varchar('region_so', { length: 100 }),
        subbranchSo: varchar('subbranch_so', { length: 100 }),
        branchSo: varchar('branch_so', { length: 100 }),
        clusterSo: varchar('cluster_so', { length: 100 }),
        kabSo: varchar('kab_so', { length: 100 }),
        kecamatanSo: varchar('kecamatan_so', { length: 100 }),
        deno: varchar('deno', { length: 100 }),
        packageCategory: varchar('package_category', { length: 100 }),
        packageGroup: varchar('package_group', { length: 100 }),
        packageSubgroup: varchar('package_subgroup', { length: 100 }),
        contentId: varchar('content_id', { length: 100 }),
        so: varchar({ length: 100 }),
        rev: decimal({ precision: 38, scale: 2 })
    }, t => [
        index('so').on(t.so).using('btree')
    ])
}