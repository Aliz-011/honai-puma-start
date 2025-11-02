import { mysqlSchema, varchar } from "drizzle-orm/mysql-core";

export const zz_wisnu = mysqlSchema('zz_wisnu')

export const wlProgCeriaAll = zz_wisnu.table('WL_Prog_Ceria_202404_All', {
    msisdn: varchar('msisdn', { length: 25 }),
    eligibility: varchar('eligibility', { length: 30 }),
    avg_arpu: varchar('avg_arpu', { length: 20 }),
    delta_payload: varchar('delta_payload', { length: 25 }),
    arpu_15k: varchar('arpu_15k', { length: 25 }),
    arpu_40k: varchar('arpu_40k', { length: 25 }),
    payload: varchar('payload', { length: 25 }),
    avg_arpu_segment: varchar('avg_arpu_segment', { length: 30 }),
    payload_movement: varchar('payload_movement', { length: 30 }),
    payload_m1: varchar('payload_m1', { length: 25 }),
    payload_m2: varchar('payload_m2', { length: 25 }),
    flag_whitelist: varchar('flag_whitelist', { length: 20 }),
    notel_check: varchar('notel_check', { length: 25 }),
    rn: varchar('rn', { length: 15 }),
    date_ps: varchar('date_ps', { length: 20 }),
    sto: varchar('sto', { length: 20 }),
    branch: varchar('branch', { length: 20 }),
    los: varchar('los', { length: 15 }),
    single_journey: varchar('single_journey', { length: 15 }),
    eligible: varchar('eligible', { length: 35 }),
})