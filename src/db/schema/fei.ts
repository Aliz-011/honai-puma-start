import { mysqlSchema, varchar, int } from "drizzle-orm/mysql-core";

export const fei = mysqlSchema('fei')

export const fei_actual_wl_trade_in = fei.table('fei_actual_wl_trade_in', {
    timestamp: varchar('timestamp', { length: 100 }),
    cat: varchar('cat', { length: 100 }),
    outlet_id_ds: varchar('outlet_id_ds', { length: 100 }),
    nama_ds: varchar('nama_ds', { length: 100 }),
    nama_pelanggan: varchar('nama_pelanggan', { length: 100 }),
    pekerjaan_pelanggan: varchar('pekerjaan_pelanggan', { length: 100 }),
    msisdn_competitor: varchar('msisdn_competitor', { length: 100 }),
    msisdn_trade_in: varchar('msisdn_trade_in', { length: 100 }),
    kota: varchar('kota', { length: 100 }),
    dokumentasi: varchar('dokumentasi', { length: 100 }),
    competitor_los: varchar('competitor_los', { length: 100 })
})

export const fei_wl_compete_puma_analysis = fei.table('fei_wl_compete_puma_analysis', {
    periode: varchar('periode', { length: 10 }),
    event_date: varchar('event_date', { length: 10 }),
    msisdn: varchar('msisdn', { length: 20 }),
    wl_check_system: varchar('wl_check_system', { length: 20 }),
    site_id: varchar('site_id', { length: 30 }),
    kecamatan: varchar('kecamatan', { length: 50 }),
    kabupaten: varchar('kabupaten', { length: 50 }),
    suspect_competitor: varchar('suspect_competitor', { length: 50 }),
    apps_competitor: varchar('apps_competitor', { length: 50 }),
    status: varchar('status', { length: 50 }),
    device_type: varchar('device_type', { length: 50 }),
    mytsel_user_flag: varchar('mytsel_user_flag', { length: 50 }),
    segment_los: varchar('segment_los', { length: 50 }),
    segment_arpu_all: varchar('segment_arpu_all', { length: 50 }),
    segment_arpu_data: varchar('segment_arpu_data', { length: 50 }),
    segment_arpu_data_pack: varchar('segment_arpu_data_pack', { length: 50 }),
    channel_new_before: varchar('channel_new_before', { length: 50 }),
    channel_new_after: varchar('channel_new_after', { length: 50 }),
    package_group_before: varchar('package_group_before', { length: 50 }),
    package_group_after: varchar('package_group_after', { length: 50 }),
    package_subgroup_before: varchar('package_subgroup_before', { length: 50 }),
    package_subgroup_after: varchar('package_subgroup_after', { length: 50 }),
    package_category_before: varchar('package_category_before', { length: 100 }),
    package_category_after: varchar('package_category_after', { length: 100 }),
    cat_taker_ss_compete_before: varchar('cat_taker_ss_compete_before', { length: 50 }),
    cat_taker_ss_compete_after: varchar('cat_taker_ss_compete_after', { length: 50 }),
    trx_data_pack_before: int('trx_data_pack_before'),
    trx_data_pack_after: int('trx_data_pack_after'),
    rev_data_pack_before: varchar('rev_data_pack_before', { length: 50 }),
    rev_data_pack_after: varchar('rev_data_pack_after', { length: 50 }),
    rev_before: varchar('rev_before', { length: 50 }),
    rev_after: varchar('rev_after', { length: 50 }),
    rev_data_before: varchar('rev_data_before', { length: 50 }),
    rev_data_after: varchar('rev_data_after', { length: 50 }),
})