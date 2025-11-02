import { index, mysqlSchema, varchar } from "drizzle-orm/mysql-core";

export const hadoop = mysqlSchema("hadoop");

export const dynamicLacciSiteId = (period: string) => {
    return hadoop.table(`lacci_siteid_${period}`, {
        event_date: varchar({ length: 20 }),
        msisdn: varchar({ length: 18 }),
        lacci: varchar({ length: 20 }),
        site_id: varchar({ length: 20 }),
        site_name: varchar({ length: 30 }),
        branch: varchar({ length: 30 }),
        cluster: varchar({ length: 30 }),
        propinsi: varchar({ length: 30 }),
        kabupaten: varchar({ length: 30 }),
        kecamatan: varchar({ length: 40 }),
        kelurahan: varchar({ length: 40 }),
        period: varchar({ length: 20 }),
    }, t => [
        index('msisdn').on(t.msisdn).using('btree'),
        index('site_id').on(t.site_id).using('btree'),
        index('kabupaten').on(t.kabupaten).using('btree')
    ])
}