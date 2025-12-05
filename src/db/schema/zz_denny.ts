import { mysqlSchema, varchar, int } from "drizzle-orm/mysql-core";

export const zz_denny = mysqlSchema('zz_denny')

export const dynamicWLWABLASTBroadband = (period: string) => {
    return zz_denny.table(`WL_WA_Blast_Broadband_${period}`, {
        msisdn: varchar({ length: 20 }),
        region: varchar({ length: 30 }),
        branch: varchar({ length: 30 }),
        cluster: varchar({ length: 30 }),
        city: varchar({ length: 30 }),
        segment_los: varchar({ length: 30 }),
        stats_pu_m1: varchar({ length: 30 }),
        pu_segments: varchar({ length: 30 }),
        service_m1: varchar({ length: 30 }),
        objectives: varchar({ length: 30 }),
        pu_m1: varchar({ length: 2 }),
        pu_mtd: varchar({ length: 2 }),
        pu_data_m1: varchar({ length: 2 }),
        pu_data_mtd: varchar({ length: 2 }),
        pu_voice_m1: varchar({ length: 2 }),
        pu_voice_mtd: varchar({ length: 2 }),
        pu_digital_m1: varchar({ length: 2 }),
        pu_digital_mtd: varchar({ length: 2 }),
        rev_m1: int(),
        rev_mtd: int(),
        rev_data_m1: int(),
        rev_data_mtd: int(),
    })
}

export const dynamicChannelWaBroadband = (period: string) => {
    return zz_denny.table(`Channel_WA_Broadband_${period}`, {
        Tahun: varchar({ length: 4 }),
        Bulan: varchar({ length: 4 }),
        Tanggal: varchar({ length: 2 }),
        Kecamatan: varchar({ length: 40 }),
        Cluster: varchar({ length: 30 }),
        Branch: varchar({ length: 30 }),
        Region: varchar({ length: 30 }),
        MSISDN_Pelanggan: varchar({ length: 20 }),
        Status_WA: varchar({ length: 10 }),
        MSISDN_for_WA: varchar({ length: 20 }),
        Kategori: varchar({ length: 20 }),
        Penawaran: varchar({ length: 100 }),
        Nama_Tim_WA: varchar({ length: 25 }),
        Eksekusi_at: varchar({ length: 25 }),
        Date_WA: varchar({ length: 10 }),
        Date: varchar({ length: 10 }),
        User: varchar({ length: 30 }),
        Time: varchar({ length: 50 }),
        ID_Campaign: varchar({ length: 25 }),
    })
}

export const wifi_lapser_prevention_puma = zz_denny.table('wifi_lapser_prevention_puma', {
    periode: varchar('periode', { length: 10 }),
    msisdn: varchar('msisdn', { length: 17 }),
    segment: varchar('segment', { length: 20 }),
    unk: varchar('unk', { length: 20 }),
    SID: varchar('SID', { length: 10 }),
})