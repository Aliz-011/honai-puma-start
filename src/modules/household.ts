import { endOfMonth, format, getDaysInMonth, subDays, subMonths, subYears, startOfMonth, parseISO } from "date-fns";
import { and, count, eq, inArray, or, sql, sum, isNotNull, getTableColumns, notInArray, gt } from "drizzle-orm";
import { Hono } from "hono";
import { z } from "zod"
import { zValidator } from "@hono/zod-validator";

import { targetHouseholdAll, summaryIoRePsBranch, summaryIoRePsWok, targetTerritoryDemands, summaryHouseholdC3mrBranch, targetRevenueC3mr, summaryIoRePsRegional, summaryAllConsolidationBranch, summaryAllConsolidationCluster, summaryAllConsolidationKabupaten, summaryBBConsolidationBranch, summaryBBConsolidationCluster, summaryBBConsolidationKabupaten, summaryPayloadConsolidationBranch, summaryPayloadConsolidationCluster, summaryPayloadConsolidationKabupaten, summaryGoliveRegional, summaryGolivePortRegional, summaryOdpRegional } from "@/db/schema/v_honai_puma";
import { dynamicIhAvailOdpA4, ih_demand_mysiis, ih_occ_golive_ihld, dynamicIhOrderingDetailOrderTable, ih_information_odp, dynamicIhC3mr } from '@/db/schema/household'
import { territoryArea4, territoryHousehold } from '@/db/schema/puma_2025'
import { db } from "@/db";
import { unionAll } from "drizzle-orm/mysql-core";

const app = new Hono()
    .get('/io-re-ps', zValidator('query', z.object({ date: z.string().optional(), branch: z.string().optional(), wok: z.string().optional() })),
        async c => {
            const { date, branch, wok } = c.req.valid('query')
            const selectedDate = date ? parseISO(date) : subDays(new Date(), 1)

            const currMonth = format(selectedDate, 'MM')
            const currYear = format(selectedDate, 'yyyy')
            const lastDayOfSelectedMonth = endOfMonth(selectedDate);
            const isEndOfMonth = selectedDate.getDate() === lastDayOfSelectedMonth.getDate();

            const endOfCurrMonth = isEndOfMonth ? lastDayOfSelectedMonth : selectedDate;
            const endOfPrevMonth = isEndOfMonth ? endOfMonth(subMonths(selectedDate, 1)) : subMonths(selectedDate, 1);
            const endOfPrevYearSameMonth = isEndOfMonth ? endOfMonth(subYears(selectedDate, 1)) : subYears(selectedDate, 1);

            const ihOrderingDetailOrder = dynamicIhOrderingDetailOrderTable(currYear, currMonth)

            const currStartOfMonth = format(startOfMonth(selectedDate), 'yyyy-MM-dd')
            const prevStartOfMonth = format(startOfMonth(endOfPrevMonth), 'yyyy-MM-dd')
            const currDate = format(selectedDate, 'yyyy-MM-dd')
            const prevDate = format(endOfPrevMonth, 'yyyy-MM-dd');
            const prevYearCurrDate = format(endOfPrevYearSameMonth, 'yyyy-MM-dd');
            const yyyyMM = format(selectedDate, 'yyyyMM')
            const daysInMonth = getDaysInMonth(selectedDate)
            const today = Number(format(selectedDate, 'd'))
            const thisMonth = Number(format(selectedDate, 'M'))
            const prevMonth = thisMonth === 1 ? 12 : thisMonth - 1
            const remainingDays = daysInMonth - today

            const regionalTerritory = db
                .select({
                    regional: territoryHousehold.regional
                })
                .from(territoryHousehold)
                .where(eq(territoryHousehold.regional, 'MALUKU DAN PAPUA'))
                .groupBy(sql`1`)
                .as('a')

            const [ioreps_regional] = await db
                .select({
                    event_date: sql<string>`COALESCE(
                        (SELECT event_date FROM ${summaryIoRePsRegional} WHERE event_date = ${currDate} LIMIT 1),
                        (SELECT MAX(event_date) FROM ${summaryIoRePsRegional})
                    )`,

                })
                .from(summaryIoRePsRegional)
                .limit(1)

            const summaryRegional = db
                .select()
                .from(summaryIoRePsRegional)
                .where(and(
                    eq(summaryIoRePsRegional.event_date, ioreps_regional.event_date),
                    eq(summaryIoRePsRegional.channel, 'all'),
                    eq(summaryIoRePsRegional.package, 'all'),
                ))
                .groupBy(summaryIoRePsRegional.regional)
                .as('b')

            const regionalTargetRevenue = db
                .select({
                    regional: territoryHousehold.regional,
                    target_all_sales: sum(targetHouseholdAll.all_sales).as('target_all_sales'),
                    target_grapari: sum(targetHouseholdAll.grapari).as('target_grapari'),
                    target_agency: sum(targetHouseholdAll.agency).as('target_agency'),
                    target_community: sum(targetHouseholdAll.community).as('target_community'),
                    target_digital: sum(targetHouseholdAll.digital).as('target_digital'),
                    target_greenfield: sum(targetHouseholdAll.greenfield).as('target_greenfield'),
                    target_brownfield: sum(targetHouseholdAll.brownfield).as('target_brownfield')
                })
                .from(targetHouseholdAll)
                .rightJoin(territoryHousehold, eq(targetHouseholdAll.territory, territoryHousehold.sto))
                .where(eq(targetHouseholdAll.periode, yyyyMM))
                .groupBy(territoryHousehold.regional)
                .as('c')

            const regionalPSByChannel = db
                .select({
                    regional: ihOrderingDetailOrder.region,
                    grapari: sql<number>`ROUND(SUM(CASE WHEN order_channel IN ('k4', 'k3') THEN 1 ELSE 0 END) * 100.0 / COUNT(order_id), 2)`.as('grapari'),
                    total_grapari: sql<number>`COUNT(CASE WHEN order_channel IN ('k4', 'k3') THEN order_id END)`.as('total_grapari'),

                    indihome: sql<number>`ROUND(SUM(CASE WHEN order_channel IN ('b0') THEN 1 ELSE 0 END) * 100.0 / COUNT(order_id), 2)`.as('indihome'),
                    total_digital: sql<number>`COUNT(CASE WHEN order_channel IN ('b0') THEN order_id END)`.as('total_digital'),

                    lp_others: sql<number>`ROUND(SUM(CASE WHEN order_channel IN ('i1', 'a0', 's3', 'b3', 'u2', 'ab') THEN 1 ELSE 0 END) * 100.0 / COUNT(order_id), 2)`.as('lp_others'),
                    total_community: sql<number>`COUNT(CASE WHEN order_channel IN ('i1', 'a0', 's3', 'b3', 'u2', 'ab') THEN order_id END)`.as('total_community'),

                    sales_force: sql<number>`ROUND(SUM(CASE WHEN order_channel IN ('i4') THEN 1 ELSE 0 END) * 100.0 / COUNT(order_id), 2)`.as('sales_force'),
                    total_agency: sql<number>`COUNT(CASE WHEN order_channel IN ('i4') THEN order_id END)`.as('total_agency')
                })
                .from(ihOrderingDetailOrder)
                .where(and(
                    eq(ihOrderingDetailOrder.region, 'MALUKU DAN PAPUA'),
                    eq(ihOrderingDetailOrder.order_type, 'NEW SALES'),
                    inArray(ihOrderingDetailOrder.funneling_group, ['COMPLETED', 'PS']),
                    sql`DATE_FORMAT(${ihOrderingDetailOrder.ps_ts}, '%Y-%m-%d') BETWEEN ${currStartOfMonth} AND ${currDate}`
                ))
                .groupBy(sql`1`)
                .as('d')

            const regionalBrownGreen = db
                .select({
                    regional: ihOrderingDetailOrder.region,
                    greenfield: sql<number>`COUNT(CASE WHEN (${ih_occ_golive_ihld.odp_name} IS NOT NULL AND ${ih_occ_golive_ihld.tahun_program} = 2025 AND YEAR(${ih_occ_golive_ihld.tanggal_go_live_uim}) = 2025) THEN ${ihOrderingDetailOrder.order_id} END)`.as('greenfield'),
                    brownfield: sql<number>`COUNT(CASE WHEN (${ih_occ_golive_ihld.odp_name} IS NOT NULL AND ${ih_occ_golive_ihld.tahun_program} = 2024 AND YEAR(${ih_occ_golive_ihld.tanggal_go_live_uim}) = 2024) THEN ${ihOrderingDetailOrder.order_id} END)`.as('brownfield'),
                })
                .from(ihOrderingDetailOrder)
                .leftJoin(ih_information_odp, eq(ihOrderingDetailOrder.service_id, ih_information_odp.service_number))
                .leftJoin(ih_occ_golive_ihld, eq(ih_information_odp.odp_name, ih_occ_golive_ihld.odp_name))
                .where(and(
                    eq(ih_occ_golive_ihld.telkomsel_regional, 'MALUKU DAN PAPUA'),
                    isNotNull(ihOrderingDetailOrder.ps_ts),
                    sql`DATE_FORMAT(${ihOrderingDetailOrder.ps_ts}, '%Y-%m-%d') BETWEEN ${currStartOfMonth} AND ${currDate}`
                ))
                .groupBy(sql`1`)
                .as('e')

            const regionalDeduplicatedData = db
                .select({
                    ...getTableColumns(ihOrderingDetailOrder),
                    duplicate_count: sql<number>`COUNT(*) OVER (PARTITION BY service_id)`.as('duplicate_count'),
                    completed_flag: sql<number>`SUM(CASE WHEN funneling_group IN ('COMPLETED', 'PS', 'CANCELLED') THEN 1 ELSE 0 END) OVER (PARTITION BY service_id)`.as('completed_flag')
                })
                .from(ihOrderingDetailOrder)
                .where(and(
                    eq(ihOrderingDetailOrder.region, 'MALUKU DAN PAPUA'),
                    eq(ihOrderingDetailOrder.order_type, 'NEW SALES'),
                    sql`DATE_FORMAT(${ihOrderingDetailOrder.re_ts}, '%Y-%m-%d') BETWEEN ${prevStartOfMonth} AND ${currDate}`,
                ))
                .as('deduplicated_data')

            const regionalFinalDeduplicatedData = db
                .select()
                .from(regionalDeduplicatedData)
                .where(and(
                    eq(regionalDeduplicatedData.duplicate_count, 1),
                    eq(regionalDeduplicatedData.completed_flag, 0),
                    notInArray(regionalDeduplicatedData.funneling_group, ['COMPLETED', 'PS', 'CANCELLED'])
                ))
                .as('final_deduplicated')

            const regionalReNonPS = db
                .select({
                    regional: regionalFinalDeduplicatedData.region,
                    registration: sql<number>`COUNT(CASE WHEN funneling_group = 'REGISTRATION' THEN order_id END)`.as('registration'),
                    provision_issued: sql<number>`COUNT(CASE WHEN funneling_group = 'PROVISION_ISSUED' THEN order_id END)`.as('provision_issued'),
                    provision_completed: sql<number>`COUNT(CASE WHEN funneling_group = 'PROVISION_COMPLETED' THEN order_id END)`.as('provision_completed'),
                    activation_completed: sql<number>`COUNT(CASE WHEN funneling_group = 'ACTIVATION_COMPLETED' THEN order_id END)`.as('activation_completed'),
                    fallout: sql<number>`COUNT(CASE WHEN funneling_group = 'FALLOUT' THEN order_id END)`.as('fallout'),
                    cancelled: sql<number>`COUNT(CASE WHEN funneling_group = 'CANCELLED' THEN order_id END)`.as('cancelled'),
                    total: sql<number>`COUNT(CASE WHEN funneling_group IN ('REGISTRATION', 'PROVISION_ISSUED', 'PROVISION_COMPLETED', 'ACTIVATION_COMPLETED', 'FALLOUT', 'CANCELLED') THEN order_id END)`.as('total'),

                    kendala_pelanggan: sql<number>`COUNT(CASE WHEN funneling_group = 'FALLOUT' AND fallout_category = 'KENDALA PELANGGAN' THEN order_id END)`.as('kendala_pelanggan'),
                    kendala_sistem: sql<number>`COUNT(CASE WHEN funneling_group = 'FALLOUT' AND fallout_category = 'KENDALA SISTEM' THEN order_id END)`.as('kendala_sistem'),
                    kendala_teknik: sql<number>`COUNT(CASE WHEN funneling_group = 'FALLOUT' AND fallout_category = 'KENDALA TEKNIK' THEN order_id END)`.as('kendala_teknik'),
                    kendala_others: sql<number>`COUNT(CASE WHEN funneling_group = 'FALLOUT' AND fallout_category IN ('OTHERS', 'KENDALA LAINNYA') THEN order_id END)`.as('kendala_others'),
                    total_all_kendala: sql<number>`COUNT(CASE WHEN funneling_group = 'FALLOUT' AND fallout_category IN ('KENDALA PELANGGAN', 'KENDALA TEKNIK', 'KENDALA SISTEM', 'OTHERS', 'KENDALA LAINNYA') THEN order_id END)`.as('total_all_kendala'),

                    // KENDALA PELANGGAN
                    INDIKASI_CABUT_PASANG: sql<number>`COUNT(CASE WHEN fallout_category = 'KENDALA PELANGGAN' AND fallout_subcategory = 'INDIKASI CABUT PASANG' THEN order_id END)`.as('INDIKASI_CABUT_PASANG'),
                    PENDING: sql<number>`COUNT(CASE WHEN fallout_category = 'KENDALA PELANGGAN' AND fallout_subcategory = 'PENDING' THEN order_id END)`.as('PENDING'),
                    PELANGGAN_MASIH_RAGU: sql<number>`COUNT(CASE WHEN fallout_category = 'KENDALA PELANGGAN' AND fallout_subcategory = 'PELANGGAN MASIH RAGU' THEN order_id END)`.as('PELANGGAN_MASIH_RAGU'),
                    RNA: sql<number>`COUNT(CASE WHEN fallout_category = 'KENDALA PELANGGAN' AND fallout_subcategory = 'RNA' THEN order_id END)`.as('RNA'),
                    KENDALA_IZIN: sql<number>`COUNT(CASE WHEN fallout_category = 'KENDALA PELANGGAN' AND fallout_subcategory = 'KENDALA IZIN' THEN order_id END)`.as('KENDALA_IZIN'),
                    BATAL: sql<number>`COUNT(CASE WHEN fallout_category = 'KENDALA PELANGGAN' AND fallout_subcategory = 'BATAL' THEN order_id END)`.as('BATAL'),
                    RUMAH_KOSONG: sql<number>`COUNT(CASE WHEN fallout_category = 'KENDALA PELANGGAN' AND fallout_subcategory = 'RUMAH KOSONG' THEN order_id END)`.as('RUMAH_KOSONG'),
                    DOUBLE_INPUT: sql<number>`COUNT(CASE WHEN fallout_category = 'KENDALA PELANGGAN' AND fallout_subcategory = 'DOUBLE INPUT' THEN order_id END)`.as('DOUBLE_INPUT'),
                    GANTI_PAKET: sql<number>`COUNT(CASE WHEN fallout_category = 'KENDALA PELANGGAN' AND fallout_subcategory = 'GANTI PAKET' THEN order_id END)`.as('GANTI_PAKET'),
                    ALAMAT_TIDAK_DITEMUKAN: sql<number>`COUNT(CASE WHEN fallout_category = 'KENDALA PELANGGAN' AND fallout_subcategory = 'ALAMAT TIDAK DITEMUKAN' THEN order_id END)`.as('ALAMAT_TIDAK_DITEMUKAN'),

                    // KENDALA TEKNIK
                    SALAH_TAGGING: sql<number>`COUNT(CASE WHEN fallout_category = 'KENDALA TEKNIK' AND fallout_subcategory = 'SALAH TAGGING' THEN order_id END)`.as('SALAH_TAGGING'),
                    ODP_RUSAK: sql<number>`COUNT(CASE WHEN fallout_category = 'KENDALA TEKNIK' AND fallout_subcategory = 'ODP RUSAK' THEN order_id END)`.as("ODP_RUSAK"),
                    ODP_FULL: sql<number>`COUNT(CASE WHEN fallout_category = 'KENDALA TEKNIK' AND fallout_subcategory = 'ODP FULL' THEN order_id END)`.as("ODP_FULL"),
                    TIANG: sql<number>`COUNT(CASE WHEN fallout_category = 'KENDALA TEKNIK' AND fallout_subcategory = 'TIANG' THEN order_id END)`.as("TIANG"),
                    CROSS_JALAN: sql<number>`COUNT(CASE WHEN fallout_category = 'KENDALA TEKNIK' AND fallout_subcategory = 'CROSS JALAN' THEN order_id END)`.as("CROSS_JALAN"),
                    TIDAK_ADA_ODP: sql<number>`COUNT(CASE WHEN fallout_category = 'KENDALA TEKNIK' AND fallout_subcategory = 'TIDAK ADA ODP' THEN order_id END)`.as("TIDAK_ADA_ODP"),
                    ODP_JAUH: sql<number>`COUNT(CASE WHEN fallout_category = 'KENDALA TEKNIK' AND fallout_subcategory = 'ODP JAUH' THEN order_id END)`.as("ODP_JAUH"),
                    ODP_BELUM_GO_LIVE: sql<number>`COUNT(CASE WHEN fallout_category = 'KENDALA TEKNIK' AND fallout_subcategory = 'ODP BELUM GO LIVE' THEN order_id END)`.as('ODP_BELUM_GO_LIVE'),
                    ODP_RETI: sql<number>`COUNT(CASE WHEN fallout_category = 'KENDALA TEKNIK' AND fallout_subcategory = 'ODP RETI' THEN order_id END)`.as('ODP_RETI'),
                    LIMITASI_ONU: sql<number>`COUNT(CASE WHEN fallout_category = 'KENDALA TEKNIK' AND fallout_subcategory = 'LIMITASI ONU' THEN order_id END)`.as('LIMITASI_ONU'),
                    JALUR_RUTE: sql<number>`COUNT(CASE WHEN fallout_category = 'KENDALA TEKNIK' AND fallout_subcategory IN ('KENDALA JALUR/RUTE TARIKAN', 'KENDALA JALUR/RUTE', 'KENDALA JALUR') THEN order_id END)`.as('JALUR_RUTE'),
                    KENDALA_IKR_IKG: sql<number>`COUNT(CASE WHEN fallout_category = 'KENDALA TEKNIK' AND fallout_subcategory = 'KENDALA IKR/IKG' THEN order_id END)`.as('KENDALA_IKR_IKG'),

                    // -LoS
                    wo_3: sql<number>`COUNT(CASE WHEN FLOOR(provi_duration) BETWEEN 0 AND 3 THEN order_id END)`.as('wo_3'),
                    wo_3_per: sql<string>`CONCAT(ROUND(SUM(CASE WHEN FLOOR(provi_duration) BETWEEN 0 AND 3 THEN 1 ELSE 0 END) * 100 / COUNT(order_id), 2), '%')`.as('wo_3_per'),
                    wo_4_7: sql<number>`COUNT(CASE WHEN FLOOR(provi_duration) BETWEEN 4 AND 7 THEN order_id END)`.as('wo_4_7'),
                    wo_4_7_per: sql<string>`CONCAT(ROUND(SUM(CASE WHEN FLOOR(provi_duration) BETWEEN 4 AND 7 THEN 1 ELSE 0 END) * 100 / COUNT(order_id), 2), '%')`.as('wo_4_7_per'),
                    wo_8_14: sql<number>`COUNT(CASE WHEN FLOOR(provi_duration) BETWEEN 8 AND 14 THEN order_id END)`.as('wo_8_14'),
                    wo_8_14_per: sql<string>`CONCAT(ROUND(SUM(CASE WHEN FLOOR(provi_duration) BETWEEN 8 AND 14 THEN 1 ELSE 0 END) * 100 / COUNT(order_id), 2), '%')`.as('wo_8_14_per'),
                    wo_15_30: sql<number>`COUNT(CASE WHEN FLOOR(provi_duration) BETWEEN 15 AND 30 THEN order_id END)`.as('wo_15_30'),
                    wo_15_30_per: sql<string>`CONCAT(ROUND(SUM(CASE WHEN FLOOR(provi_duration) BETWEEN 15 AND 30 THEN 1 ELSE 0 END) * 100 / COUNT(order_id), 2), '%')`.as('wo_15_30_per'),
                    wo_gt_30: sql<number>`COUNT(CASE WHEN FLOOR(provi_duration) > 30 THEN order_id END)`.as('wo_gt_30'),
                    wo_gt_30_per: sql<string>`CONCAT(ROUND(SUM(CASE WHEN FLOOR(provi_duration) > 30 THEN 1 ELSE 0 END) * 100 / COUNT(order_id), 2), '%')`.as('wo_gt_30_per'),
                    total_wo: sql<number>`COUNT(CASE WHEN provi_duration IS NOT NULL THEN order_id END)`.as('total_wo')
                })
                .from(regionalFinalDeduplicatedData)
                .groupBy(sql`1`)
                .as('f')

            const revenueRegional = db
                .select({
                    name: regionalTerritory.regional,
                    target_all_sales: sql<number>`ROUND(SUM(${regionalTargetRevenue.target_all_sales}),2)`.as('target_all_sales'),
                    level: sql<string>`'regional'`.as('level'),

                    ach_target_fm_io_all_sales: sql<number>`ROUND((SUM(${summaryRegional.io_m})/SUM(${regionalTargetRevenue.target_all_sales}))*100, 2)`.as('ach_target_fm_io_all_sales'),
                    drr_io: sql<number>`ROUND((SUM(${summaryRegional.io_m})/(${today}/${daysInMonth}*SUM(${regionalTargetRevenue.target_all_sales})))*100, 2)`.as('drr_io'),
                    io_m: sql<number>`ROUND(SUM(${summaryRegional.io_m}),2)`.as('io_m'),
                    io_mom: sql<string>`CONCAT(${summaryRegional.io_mom}, '%')`.as('io_mom'),
                    io_gap_daily: sql<number>`ROUND(SUM(${summaryRegional.io_gap_daily}),2)`.as('io_gap_daily'),

                    ach_target_fm_re_all_sales: sql<number>`ROUND((SUM(${summaryRegional.re_m})/SUM(${regionalTargetRevenue.target_all_sales}))*100, 2)`.as('ach_target_fm_re_all_sales'),
                    drr_re: sql<number>`ROUND((SUM(${summaryRegional.re_m})/(${today}/${daysInMonth}*SUM(${regionalTargetRevenue.target_all_sales})))*100, 2)`.as('drr_re'),
                    re_m: sql<number>`ROUND(SUM(${summaryRegional.re_m}),2)`.as('re_m'),
                    re_mom: sql<string>`CONCAT(${summaryRegional.re_mom}, '%')`.as('re_mom'),
                    re_gap_daily: sql<number>`ROUND(SUM(${summaryRegional.re_gap_daily}),2)`.as('re_gap_daily'),

                    ach_target_fm_ps_all_sales: sql<number>`ROUND((SUM(${summaryRegional.ps_m})/SUM(${regionalTargetRevenue.target_all_sales}))*100, 2)`.as('ach_target_fm_ps_all_sales'),
                    drr_ps: sql<number>`ROUND((SUM(${summaryRegional.ps_m})/(${today}/${daysInMonth}*SUM(${regionalTargetRevenue.target_all_sales})))*100, 2)`.as('drr_ps'),
                    target_daily_ps: sql<number>`ROUND(SUM(${regionalTargetRevenue.target_all_sales}/${daysInMonth}), 2)`.as('target_daily_ps'),
                    ach_daily_ps: sql<number>`ROUND(SUM(${summaryRegional.ps_m}/${today}), 2)`.as('ach_daily_ps'),
                    ps_gap_daily: sql<number>`ROUND(SUM(${summaryRegional.ps_gap_daily}), 2)`.as('ps_gap_daily'),
                    daily_ps_remaining: sql<number>`ROUND(SUM(${regionalTargetRevenue.target_all_sales} - ${summaryRegional.ps_m})/${remainingDays},2)`.as('daily_ps_remaining'),
                    ps_m: sql<number>`ROUND(SUM(${summaryRegional.ps_m}),2)`.as('ps_m'),
                    ps_mom: sql<string>`CONCAT(${summaryRegional.ps_mom}, '%')`.as('ps_mom'),
                    ps_to_io: sql<number>`CONCAT(${summaryRegional.ps_to_io}, '%')`.as('ps_to_io'),
                    ps_to_re: sql<number>`CONCAT(${summaryRegional.ps_to_re}, '%')`.as('ps_to_re'),

                    ach_fm_indihome: sql<string>`CONCAT(d.indihome,'%')`.as('ach_fm_indihome'),
                    ps_indihome: sql<number>`ROUND(d.total_digital, 2)`.as('ps_indihome'),
                    ach_fm_grapari: sql<string>`CONCAT(d.grapari,'%')`.as('ach_fm_grapari'),
                    ps_grapari: sql<number>`ROUND(d.total_grapari, 2)`.as('ps_grapari'),
                    ach_fm_community: sql<string>`CONCAT(d.lp_others,'%')`.as('ach_fm_community'),
                    ps_community: sql<number>`ROUND(d.total_community, 2)`.as('ps_community'),
                    ach_fm_agency: sql<string>`CONCAT(d.sales_force,'%')`.as('ach_fm_agency'),
                    ps_sales_force: sql<number>`ROUND(d.total_agency, 2)`.as('ps_agency'),

                    brownfield: sql<number>`ROUND(SUM(e.brownfield), 2)`.as('brownfield'),
                    target_brownfield: sql<number>`ROUND(SUM(c.target_brownfield),2)`.as('target_brownfield'),
                    ach_fm_brownfield: sql<string>`CONCAT(ROUND((SUM(e.brownfield)/SUM(c.target_brownfield))*100, 2), '%')`.as('ach_fm_brownfield'),
                    drr_brownfield: sql<string>`CONCAT(ROUND((SUM(e.brownfield)/(${today}/${daysInMonth}*SUM(c.target_brownfield)))*100, 2), '%')`.as('drr_brownfield'),
                    greenfield: sql<number>`ROUND(SUM(e.greenfield), 2)`.as('greenfield'),
                    target_greenfield: sql<number>`ROUND(SUM(c.target_greenfield),2)`.as('target_greenfield'),
                    ach_fm_greenfield: sql<string>`CONCAT(ROUND((SUM(e.greenfield)/SUM(c.target_greenfield))*100, 2), '%')`.as('ach_fm_greenfield'),
                    drr_greenfield: sql<string>`CONCAT(ROUND((SUM(e.greenfield)/(${today}/${daysInMonth}*SUM(c.target_greenfield)))*100, 2), '%')`.as('drr_greenfield'),

                    registration: regionalReNonPS.registration,
                    provision_issued: regionalReNonPS.provision_issued,
                    provision_completed: regionalReNonPS.provision_completed,
                    activation_completed: regionalReNonPS.activation_completed,
                    fallout: regionalReNonPS.fallout,
                    cancelled: regionalReNonPS.cancelled,
                    registration_per: sql<string>`CONCAT(ROUND(f.registration / f.total * 100.0, 2),'%')`.as('registration_per'),
                    provision_issued_per: sql<string>`CONCAT(ROUND(f.provision_issued / f.total * 100.0, 2), '%')`.as('provision_issued_per'),
                    provision_completed_per: sql<string>`CONCAT(ROUND(f.provision_completed / f.total * 100.0, 2), '%')`.as('provision_completed_per'),
                    activation_completed_per: sql<string>`CONCAT(ROUND(f.activation_completed / f.total * 100.0, 2), '%')`.as('activation_completed_per'),
                    fallout_per: sql<string>`CONCAT(ROUND(f.fallout / f.total * 100.0, 2), '%')`.as('fallout_per'),
                    cancelled_per: sql<string>`CONCAT(ROUND(f.cancelled / f.total * 100.0, 2), '%')`.as('cancelled_per'),
                    total_re_non_ps: sql<number>`f.total`.as('total_re_non_ps'),

                    kendala_pelanggan: regionalReNonPS.kendala_pelanggan,
                    kendala_sistem: regionalReNonPS.kendala_sistem,
                    kendala_teknik: regionalReNonPS.kendala_teknik,
                    kendala_others: regionalReNonPS.kendala_others,
                    total_all_kendala: regionalReNonPS.total_all_kendala,

                    INDIKASI_CABUT_PASANG: regionalReNonPS.INDIKASI_CABUT_PASANG,
                    PENDING: regionalReNonPS.PENDING,
                    PELANGGAN_MASIH_RAGU: regionalReNonPS.PELANGGAN_MASIH_RAGU,
                    RNA: regionalReNonPS.RNA,
                    KENDALA_IZIN: regionalReNonPS.KENDALA_IZIN,
                    BATAL: regionalReNonPS.BATAL,
                    RUMAH_KOSONG: regionalReNonPS.RUMAH_KOSONG,
                    DOUBLE_INPUT: regionalReNonPS.DOUBLE_INPUT,
                    GANTI_PAKET: regionalReNonPS.GANTI_PAKET,
                    ALAMAT_TIDAK_DITEMUKAN: regionalReNonPS.ALAMAT_TIDAK_DITEMUKAN,

                    SALAH_TAGGING: regionalReNonPS.SALAH_TAGGING,
                    ODP_RUSAK: regionalReNonPS.ODP_RUSAK,
                    ODP_FULL: regionalReNonPS.ODP_FULL,
                    TIANG: regionalReNonPS.TIANG,
                    CROSS_JALAN: regionalReNonPS.CROSS_JALAN,
                    TIDAK_ADA_ODP: regionalReNonPS.TIDAK_ADA_ODP,
                    ODP_JAUH: regionalReNonPS.ODP_JAUH,
                    ODP_BELUM_GO_LIVE: regionalReNonPS.ODP_BELUM_GO_LIVE,
                    ODP_RETI: regionalReNonPS.ODP_RETI,
                    LIMITASI_ONU: regionalReNonPS.LIMITASI_ONU,
                    JALUR_RUTE: regionalReNonPS.JALUR_RUTE,
                    KENDALA_IKR_IKG: regionalReNonPS.KENDALA_IKR_IKG,

                    wo_3: regionalReNonPS.wo_3,
                    wo_3_per: regionalReNonPS.wo_3_per,
                    wo_4_7: regionalReNonPS.wo_4_7,
                    wo_4_7_per: regionalReNonPS.wo_4_7_per,
                    wo_8_14: regionalReNonPS.wo_8_14,
                    wo_8_14_per: regionalReNonPS.wo_8_14_per,
                    wo_15_30: regionalReNonPS.wo_15_30,
                    wo_15_30_per: regionalReNonPS.wo_15_30_per,
                    wo_gt_30: regionalReNonPS.wo_gt_30,
                    wo_gt_30_per: regionalReNonPS.wo_gt_30_per,
                    total_wo: regionalReNonPS.total_wo,

                    ioreps_event_date: sql<string>`${ioreps_regional.event_date}`.as('ioreps_event_date')
                })
                .from(regionalTerritory)
                .leftJoin(summaryRegional, eq(regionalTerritory.regional, summaryRegional.regional))
                .leftJoin(regionalTargetRevenue, eq(summaryRegional.regional, regionalTargetRevenue.regional))
                .leftJoin(regionalPSByChannel, eq(regionalTerritory.regional, regionalPSByChannel.regional))
                .leftJoin(regionalBrownGreen, eq(regionalTerritory.regional, regionalBrownGreen.regional))
                .leftJoin(regionalReNonPS, eq(regionalTerritory.regional, regionalReNonPS.regional))
                .groupBy(regionalTerritory.regional)

            const branchTerritory = db
                .select({
                    branch: territoryHousehold.branch,
                })
                .from(territoryHousehold)
                .where(branch ?
                    and(
                        eq(territoryHousehold.regional, 'MALUKU DAN PAPUA'),
                        eq(territoryHousehold.branch, branch)
                    ) :
                    eq(territoryHousehold.regional, 'MALUKU DAN PAPUA')
                )
                .groupBy(territoryHousehold.branch)
                .as('a')

            const [ioreps_branch] = await db
                .select({
                    event_date: sql<string>`COALESCE(
                        (SELECT event_date FROM ${summaryIoRePsBranch} WHERE event_date = ${currDate} LIMIT 1),
                        (SELECT MAX(event_date) FROM ${summaryIoRePsBranch})
                    )`,

                })
                .from(summaryIoRePsBranch)
                .limit(1)

            const summaryBranch = db
                .select()
                .from(summaryIoRePsBranch)
                .where(and(
                    eq(summaryIoRePsBranch.event_date, ioreps_branch.event_date),
                    eq(summaryIoRePsBranch.channel, 'all'),
                    eq(summaryIoRePsBranch.package, 'all'),
                    branch ? eq(summaryIoRePsBranch.branch, branch) : undefined
                ))
                .groupBy(summaryIoRePsBranch.branch)
                .as('b')

            const branchTargetRevenue = db
                .select({
                    branch: territoryHousehold.branch,
                    target_all_sales: sum(targetHouseholdAll.all_sales).as('target_all_sales'),
                    target_grapari: sum(targetHouseholdAll.grapari).as('target_grapari'),
                    target_agency: sum(targetHouseholdAll.agency).as('target_agency'),
                    target_community: sum(targetHouseholdAll.community).as('target_community'),
                    target_digital: sum(targetHouseholdAll.digital).as('target_digital'),
                    target_greenfield: sum(targetHouseholdAll.greenfield).as('target_greenfield'),
                    target_brownfield: sum(targetHouseholdAll.brownfield).as('target_brownfield')
                })
                .from(targetHouseholdAll)
                .rightJoin(territoryHousehold, eq(targetHouseholdAll.territory, territoryHousehold.sto))
                .where(and(
                    eq(targetHouseholdAll.periode, yyyyMM),
                    branch ? eq(territoryHousehold.branch, branch) : undefined
                ))
                .groupBy(territoryHousehold.branch)
                .as('c')

            const branchPSByChannel = db
                .select({
                    branch: ihOrderingDetailOrder.branch,
                    grapari: sql<number>`ROUND(SUM(CASE WHEN order_channel IN ('k4', 'k3') THEN 1 ELSE 0 END) * 100.0 / COUNT(order_id), 2)`.as('grapari'),
                    total_grapari: sql<number>`COUNT(CASE WHEN order_channel IN ('k4', 'k3') THEN order_id END)`.as('total_grapari'),

                    indihome: sql<number>`ROUND(SUM(CASE WHEN order_channel IN ('b0') THEN 1 ELSE 0 END) * 100.0 / COUNT(order_id), 2)`.as('indihome'),
                    total_digital: sql<number>`COUNT(CASE WHEN order_channel IN ('b0') THEN order_id END)`.as('total_digital'),

                    lp_others: sql<number>`ROUND(SUM(CASE WHEN order_channel IN ('i1', 'a0', 's3', 'b3', 'u2', 'ab') THEN 1 ELSE 0 END) * 100.0 / COUNT(order_id), 2)`.as('lp_others'),
                    total_community: sql<number>`COUNT(CASE WHEN order_channel IN ('i1', 'a0', 's3', 'b3', 'u2', 'ab') THEN order_id END)`.as('total_community'),

                    sales_force: sql<number>`ROUND(SUM(CASE WHEN order_channel IN ('i4') THEN 1 ELSE 0 END) * 100.0 / COUNT(order_id), 2)`.as('sales_force'),
                    total_agency: sql<number>`COUNT(CASE WHEN order_channel IN ('i4') THEN order_id END)`.as('total_agency')
                })
                .from(ihOrderingDetailOrder)
                .where(and(
                    eq(ihOrderingDetailOrder.region, 'MALUKU DAN PAPUA'),
                    eq(ihOrderingDetailOrder.order_type, 'NEW SALES'),
                    inArray(ihOrderingDetailOrder.funneling_group, ['COMPLETED', 'PS']),
                    sql`DATE_FORMAT(${ihOrderingDetailOrder.ps_ts}, '%Y-%m-%d') BETWEEN ${currStartOfMonth} AND ${currDate}`
                ))
                .groupBy(sql`1`)
                .as('d')

            const branchBrownGreen = db
                .select({
                    branch: ihOrderingDetailOrder.branch,
                    greenfield: sql<number>`COUNT(CASE WHEN (${ih_occ_golive_ihld.odp_name} IS NOT NULL AND ${ih_occ_golive_ihld.tahun_program} = 2025 AND YEAR(${ih_occ_golive_ihld.tanggal_go_live_uim}) = 2025) THEN ${ihOrderingDetailOrder.order_id} END)`.as('greenfield'),
                    brownfield: sql<number>`COUNT(CASE WHEN (${ih_occ_golive_ihld.odp_name} IS NOT NULL AND ${ih_occ_golive_ihld.tahun_program} = 2024 AND YEAR(${ih_occ_golive_ihld.tanggal_go_live_uim}) = 2024) THEN ${ihOrderingDetailOrder.order_id} END)`.as('brownfield'),
                })
                .from(ihOrderingDetailOrder)
                .leftJoin(ih_information_odp, eq(ihOrderingDetailOrder.service_id, ih_information_odp.service_number))
                .leftJoin(ih_occ_golive_ihld, eq(ih_information_odp.odp_name, ih_occ_golive_ihld.odp_name))
                .where(and(
                    eq(ih_occ_golive_ihld.telkomsel_regional, 'MALUKU DAN PAPUA'),
                    isNotNull(ihOrderingDetailOrder.ps_ts),
                    sql`DATE_FORMAT(${ihOrderingDetailOrder.ps_ts}, '%Y-%m-%d') BETWEEN ${currStartOfMonth} AND ${currDate}`
                ))
                .groupBy(ihOrderingDetailOrder.branch)
                .as('e')

            const branchDeduplicatedData = db
                .select({
                    ...getTableColumns(ihOrderingDetailOrder),
                    duplicate_count: sql<number>`COUNT(*) OVER (PARTITION BY service_id)`.as('duplicate_count'),
                    completed_flag: sql<number>`SUM(CASE WHEN funneling_group IN ('COMPLETED', 'PS', 'CANCELLED') THEN 1 ELSE 0 END) OVER (PARTITION BY service_id)`.as('completed_flag')
                })
                .from(ihOrderingDetailOrder)
                .where(and(
                    eq(ihOrderingDetailOrder.region, 'MALUKU DAN PAPUA'),
                    eq(ihOrderingDetailOrder.order_type, 'NEW SALES'),
                    sql`DATE_FORMAT(${ihOrderingDetailOrder.re_ts}, '%Y-%m-%d') BETWEEN ${prevStartOfMonth} AND ${currDate}`,
                ))
                .as('deduplicated_data')

            const branchFinalDeduplicatedData = db
                .select()
                .from(branchDeduplicatedData)
                .where(and(
                    eq(branchDeduplicatedData.duplicate_count, 1),
                    eq(branchDeduplicatedData.completed_flag, 0),
                    notInArray(branchDeduplicatedData.funneling_group, ['COMPLETED', 'PS', 'CANCELLED'])
                ))
                .as('final_deduplicated')

            const branchReNonPS = db
                .select({
                    branch: branchFinalDeduplicatedData.branch,
                    registration: sql<number>`COUNT(CASE WHEN funneling_group = 'REGISTRATION' THEN order_id END)`.as('registration'),
                    provision_issued: sql<number>`COUNT(CASE WHEN funneling_group = 'PROVISION_ISSUED' THEN order_id END)`.as('provision_issued'),
                    provision_completed: sql<number>`COUNT(CASE WHEN funneling_group = 'PROVISION_COMPLETED' THEN order_id END)`.as('provision_completed'),
                    activation_completed: sql<number>`COUNT(CASE WHEN funneling_group = 'ACTIVATION_COMPLETED' THEN order_id END)`.as('activation_completed'),
                    fallout: sql<number>`COUNT(CASE WHEN funneling_group = 'FALLOUT' THEN order_id END)`.as('fallout'),
                    cancelled: sql<number>`COUNT(CASE WHEN funneling_group = 'CANCELLED' THEN order_id END)`.as('cancelled'),
                    total: sql<number>`COUNT(CASE WHEN funneling_group IN ('REGISTRATION', 'PROVISION_ISSUED', 'PROVISION_COMPLETED', 'ACTIVATION_COMPLETED', 'FALLOUT', 'CANCELLED') THEN order_id END)`.as('total'),

                    kendala_pelanggan: sql<number>`COUNT(CASE WHEN funneling_group = 'FALLOUT' AND fallout_category = 'KENDALA PELANGGAN' THEN order_id END)`.as('kendala_pelanggan'),
                    kendala_sistem: sql<number>`COUNT(CASE WHEN funneling_group = 'FALLOUT' AND fallout_category = 'KENDALA SISTEM' THEN order_id END)`.as('kendala_sistem'),
                    kendala_teknik: sql<number>`COUNT(CASE WHEN funneling_group = 'FALLOUT' AND fallout_category = 'KENDALA TEKNIK' THEN order_id END)`.as('kendala_teknik'),
                    kendala_others: sql<number>`COUNT(CASE WHEN funneling_group = 'FALLOUT' AND fallout_category IN ('OTHERS', 'KENDALA LAINNYA') THEN order_id END)`.as('kendala_others'),
                    total_all_kendala: sql<number>`COUNT(CASE WHEN funneling_group = 'FALLOUT' AND fallout_category IN ('KENDALA PELANGGAN', 'KENDALA TEKNIK', 'KENDALA SISTEM', 'OTHERS', 'KENDALA LAINNYA') THEN order_id END)`.as('total_all_kendala'),

                    // KENDALA PELANGGAN
                    INDIKASI_CABUT_PASANG: sql<number>`COUNT(CASE WHEN fallout_category = 'KENDALA PELANGGAN' AND fallout_subcategory = 'INDIKASI CABUT PASANG' THEN order_id END)`.as('INDIKASI_CABUT_PASANG'),
                    PENDING: sql<number>`COUNT(CASE WHEN fallout_category = 'KENDALA PELANGGAN' AND fallout_subcategory = 'PENDING' THEN order_id END)`.as('PENDING'),
                    PELANGGAN_MASIH_RAGU: sql<number>`COUNT(CASE WHEN fallout_category = 'KENDALA PELANGGAN' AND fallout_subcategory = 'PELANGGAN MASIH RAGU' THEN order_id END)`.as('PELANGGAN_MASIH_RAGU'),
                    RNA: sql<number>`COUNT(CASE WHEN fallout_category = 'KENDALA PELANGGAN' AND fallout_subcategory = 'RNA' THEN order_id END)`.as('RNA'),
                    KENDALA_IZIN: sql<number>`COUNT(CASE WHEN fallout_category = 'KENDALA PELANGGAN' AND fallout_subcategory = 'KENDALA IZIN' THEN order_id END)`.as('KENDALA_IZIN'),
                    BATAL: sql<number>`COUNT(CASE WHEN fallout_category = 'KENDALA PELANGGAN' AND fallout_subcategory = 'BATAL' THEN order_id END)`.as('BATAL'),
                    RUMAH_KOSONG: sql<number>`COUNT(CASE WHEN fallout_category = 'KENDALA PELANGGAN' AND fallout_subcategory = 'RUMAH KOSONG' THEN order_id END)`.as('RUMAH_KOSONG'),
                    DOUBLE_INPUT: sql<number>`COUNT(CASE WHEN fallout_category = 'KENDALA PELANGGAN' AND fallout_subcategory = 'DOUBLE INPUT' THEN order_id END)`.as('DOUBLE_INPUT'),
                    GANTI_PAKET: sql<number>`COUNT(CASE WHEN fallout_category = 'KENDALA PELANGGAN' AND fallout_subcategory = 'GANTI PAKET' THEN order_id END)`.as('GANTI_PAKET'),
                    ALAMAT_TIDAK_DITEMUKAN: sql<number>`COUNT(CASE WHEN fallout_category = 'KENDALA PELANGGAN' AND fallout_subcategory = 'ALAMAT TIDAK DITEMUKAN' THEN order_id END)`.as('ALAMAT_TIDAK_DITEMUKAN'),

                    // KENDALA TEKNIK
                    SALAH_TAGGING: sql<number>`COUNT(CASE WHEN fallout_category = 'KENDALA TEKNIK' AND fallout_subcategory = 'SALAH TAGGING' THEN order_id END)`.as('SALAH_TAGGING'),
                    ODP_RUSAK: sql<number>`COUNT(CASE WHEN fallout_category = 'KENDALA TEKNIK' AND fallout_subcategory = 'ODP RUSAK' THEN order_id END)`.as("ODP_RUSAK"),
                    ODP_FULL: sql<number>`COUNT(CASE WHEN fallout_category = 'KENDALA TEKNIK' AND fallout_subcategory = 'ODP FULL' THEN order_id END)`.as("ODP_FULL"),
                    TIANG: sql<number>`COUNT(CASE WHEN fallout_category = 'KENDALA TEKNIK' AND fallout_subcategory = 'TIANG' THEN order_id END)`.as("TIANG"),
                    CROSS_JALAN: sql<number>`COUNT(CASE WHEN fallout_category = 'KENDALA TEKNIK' AND fallout_subcategory = 'CROSS JALAN' THEN order_id END)`.as("CROSS_JALAN"),
                    TIDAK_ADA_ODP: sql<number>`COUNT(CASE WHEN fallout_category = 'KENDALA TEKNIK' AND fallout_subcategory = 'TIDAK ADA ODP' THEN order_id END)`.as("TIDAK_ADA_ODP"),
                    ODP_JAUH: sql<number>`COUNT(CASE WHEN fallout_category = 'KENDALA TEKNIK' AND fallout_subcategory = 'ODP JAUH' THEN order_id END)`.as("ODP_JAUH"),
                    ODP_BELUM_GO_LIVE: sql<number>`COUNT(CASE WHEN fallout_category = 'KENDALA TEKNIK' AND fallout_subcategory = 'ODP BELUM GO LIVE' THEN order_id END)`.as('ODP_BELUM_GO_LIVE'),
                    ODP_RETI: sql<number>`COUNT(CASE WHEN fallout_category = 'KENDALA TEKNIK' AND fallout_subcategory = 'ODP RETI' THEN order_id END)`.as('ODP_RETI'),
                    LIMITASI_ONU: sql<number>`COUNT(CASE WHEN fallout_category = 'KENDALA TEKNIK' AND fallout_subcategory = 'LIMITASI ONU' THEN order_id END)`.as('LIMITASI_ONU'),
                    JALUR_RUTE: sql<number>`COUNT(CASE WHEN fallout_category = 'KENDALA TEKNIK' AND fallout_subcategory IN ('KENDALA JALUR/RUTE TARIKAN', 'KENDALA JALUR/RUTE', 'KENDALA JALUR') THEN order_id END)`.as('JALUR_RUTE'),
                    KENDALA_IKR_IKG: sql<number>`COUNT(CASE WHEN fallout_category = 'KENDALA TEKNIK' AND fallout_subcategory = 'KENDALA IKR/IKG' THEN order_id END)`.as('KENDALA_IKR_IKG'),

                    // -LoS
                    wo_3: sql<number>`COUNT(CASE WHEN FLOOR(provi_duration) BETWEEN 0 AND 3 THEN order_id END)`.as('wo_3'),
                    wo_3_per: sql<string>`CONCAT(ROUND(SUM(CASE WHEN FLOOR(provi_duration) BETWEEN 0 AND 3 THEN 1 ELSE 0 END) * 100 / COUNT(order_id), 2), '%')`.as('wo_3_per'),
                    wo_4_7: sql<number>`COUNT(CASE WHEN FLOOR(provi_duration) BETWEEN 4 AND 7 THEN order_id END)`.as('wo_4_7'),
                    wo_4_7_per: sql<string>`CONCAT(ROUND(SUM(CASE WHEN FLOOR(provi_duration) BETWEEN 4 AND 7 THEN 1 ELSE 0 END) * 100 / COUNT(order_id), 2), '%')`.as('wo_4_7_per'),
                    wo_8_14: sql<number>`COUNT(CASE WHEN FLOOR(provi_duration) BETWEEN 8 AND 14 THEN order_id END)`.as('wo_8_14'),
                    wo_8_14_per: sql<string>`CONCAT(ROUND(SUM(CASE WHEN FLOOR(provi_duration) BETWEEN 8 AND 14 THEN 1 ELSE 0 END) * 100 / COUNT(order_id), 2), '%')`.as('wo_8_14_per'),
                    wo_15_30: sql<number>`COUNT(CASE WHEN FLOOR(provi_duration) BETWEEN 15 AND 30 THEN order_id END)`.as('wo_15_30'),
                    wo_15_30_per: sql<string>`CONCAT(ROUND(SUM(CASE WHEN FLOOR(provi_duration) BETWEEN 15 AND 30 THEN 1 ELSE 0 END) * 100 / COUNT(order_id), 2), '%')`.as('wo_15_30_per'),
                    wo_gt_30: sql<number>`COUNT(CASE WHEN FLOOR(provi_duration) > 30 THEN order_id END)`.as('wo_gt_30'),
                    wo_gt_30_per: sql<string>`CONCAT(ROUND(SUM(CASE WHEN FLOOR(provi_duration) > 30 THEN 1 ELSE 0 END) * 100 / COUNT(order_id), 2), '%')`.as('wo_gt_30_per'),
                    total_wo: sql<number>`COUNT(CASE WHEN provi_duration IS NOT NULL THEN order_id END)`.as('total_wo')
                })
                .from(branchFinalDeduplicatedData)
                .groupBy(sql`1`)
                .as('f')

            const revenueBranch = db
                .select({
                    name: branchTerritory.branch,
                    target_all_sales: sql<number>`ROUND(SUM(${branchTargetRevenue.target_all_sales}),2)`.as('target_all_sales'),
                    level: sql<string>`'branch'`.as('level'),

                    ach_target_fm_io_all_sales: sql<number>`ROUND((SUM(${summaryBranch.io_m})/SUM(${branchTargetRevenue.target_all_sales}))*100, 2)`.as('ach_target_fm_io_all_sales'),
                    drr_io: sql<number>`ROUND((SUM(${summaryBranch.io_m})/(${today}/${daysInMonth}*SUM(${branchTargetRevenue.target_all_sales})))*100, 2)`.as('drr_io'),
                    io_m: sql<number>`ROUND(SUM(${summaryBranch.io_m}),2)`.as('io_m'),
                    io_mom: sql<string>`CONCAT(${summaryBranch.io_mom}, '%')`.as('io_mom'),
                    io_gap_daily: sql<number>`ROUND(SUM(${summaryBranch.io_gap_daily}),2)`.as('io_gap_daily'),

                    ach_target_fm_re_all_sales: sql<number>`ROUND((SUM(${summaryBranch.re_m})/SUM(${branchTargetRevenue.target_all_sales}))*100, 2)`.as('ach_target_fm_re_all_sales'),
                    drr_re: sql<number>`ROUND((SUM(${summaryBranch.re_m})/(${today}/${daysInMonth}*SUM(${branchTargetRevenue.target_all_sales})))*100, 2)`.as('drr_re'),
                    re_m: sql<number>`ROUND(SUM(${summaryBranch.re_m}),2)`.as('re_m'),
                    re_mom: sql<string>`CONCAT(${summaryBranch.re_mom}, '%')`.as('re_mom'),
                    re_gap_daily: sql<number>`ROUND(SUM(${summaryBranch.re_gap_daily}),2)`.as('re_gap_daily'),

                    ach_target_fm_ps_all_sales: sql<number>`ROUND((SUM(${summaryBranch.ps_m})/SUM(${branchTargetRevenue.target_all_sales}))*100, 2)`.as('ach_target_fm_ps_all_sales'),
                    drr_ps: sql<number>`ROUND((SUM(${summaryBranch.ps_m})/(${today}/${daysInMonth}*SUM(${branchTargetRevenue.target_all_sales})))*100, 2)`.as('drr_ps'),
                    target_daily_ps: sql<number>`ROUND(SUM(${branchTargetRevenue.target_all_sales}/${daysInMonth}), 2)`.as('target_daily_ps'),
                    ach_daily_ps: sql<number>`ROUND(SUM(${summaryBranch.ps_m}/${today}), 2)`.as('ach_daily_ps'),
                    ps_gap_daily: sql<number>`ROUND(SUM(${summaryBranch.ps_gap_daily}), 2)`.as('ps_gap_daily'),
                    daily_ps_remaining: sql<number>`ROUND(SUM(${branchTargetRevenue.target_all_sales} - ${summaryBranch.ps_m})/${remainingDays},2)`.as('daily_ps_remaining'),
                    ps_m: sql<number>`ROUND(SUM(${summaryBranch.ps_m}),2)`.as('ps_m'),
                    ps_mom: sql<string>`CONCAT(${summaryBranch.ps_mom}, '%')`.as('ps_mom'),
                    ps_to_io: sql<number>`CONCAT(${summaryBranch.ps_to_io}, '%')`.as('ps_to_io'),
                    ps_to_re: sql<number>`CONCAT(${summaryBranch.ps_to_re}, '%')`.as('ps_to_re'),

                    ach_fm_indihome: sql<string>`CONCAT(d.indihome,'%')`.as('ach_fm_indihome'),
                    ps_indihome: sql<number>`ROUND(d.total_digital, 2)`.as('ps_indihome'),
                    ach_fm_grapari: sql<string>`CONCAT(d.grapari,'%')`.as('ach_fm_grapari'),
                    ps_grapari: sql<number>`ROUND(d.total_grapari, 2)`.as('ps_grapari'),
                    ach_fm_community: sql<string>`CONCAT(d.lp_others,'%')`.as('ach_fm_community'),
                    ps_community: sql<number>`ROUND(d.total_community, 2)`.as('ps_community'),
                    ach_fm_agency: sql<string>`CONCAT(d.sales_force,'%')`.as('ach_fm_agency'),
                    ps_sales_force: sql<number>`ROUND(d.total_agency, 2)`.as('ps_agency'),

                    brownfield: sql<number>`ROUND(SUM(e.brownfield), 2)`.as('brownfield'),
                    target_brownfield: sql<number>`ROUND(SUM(c.target_brownfield),2)`.as('target_brownfield'),
                    ach_fm_brownfield: sql<string>`CONCAT(ROUND((SUM(e.brownfield)/SUM(c.target_brownfield))*100, 2), '%')`.as('ach_fm_brownfield'),
                    drr_brownfield: sql<string>`CONCAT(ROUND((SUM(e.brownfield)/(${today}/${daysInMonth}*SUM(c.target_brownfield)))*100, 2), '%')`.as('drr_brownfield'),
                    greenfield: sql<number>`ROUND(SUM(e.greenfield), 2)`.as('greenfield'),
                    target_greenfield: sql<number>`ROUND(SUM(c.target_greenfield),2)`.as('target_greenfield'),
                    ach_fm_greenfield: sql<string>`CONCAT(ROUND((SUM(e.greenfield)/SUM(c.target_greenfield))*100, 2), '%')`.as('ach_fm_greenfield'),
                    drr_greenfield: sql<string>`CONCAT(ROUND((SUM(e.greenfield)/(${today}/${daysInMonth}*SUM(c.target_greenfield)))*100, 2), '%')`.as('drr_greenfield'),

                    registration: branchReNonPS.registration,
                    provision_issued: branchReNonPS.provision_issued,
                    provision_completed: branchReNonPS.provision_completed,
                    activation_completed: branchReNonPS.activation_completed,
                    fallout: branchReNonPS.fallout,
                    cancelled: branchReNonPS.cancelled,
                    registration_per: sql<string>`CONCAT(ROUND(f.registration / f.total * 100.0, 2),'%')`.as('registration_per'),
                    provision_issued_per: sql<string>`CONCAT(ROUND(f.provision_issued / f.total * 100.0, 2), '%')`.as('provision_issued_per'),
                    provision_completed_per: sql<string>`CONCAT(ROUND(f.provision_completed / f.total * 100.0, 2), '%')`.as('provision_completed_per'),
                    activation_completed_per: sql<string>`CONCAT(ROUND(f.activation_completed / f.total * 100.0, 2), '%')`.as('activation_completed_per'),
                    fallout_per: sql<string>`CONCAT(ROUND(f.fallout / f.total * 100.0, 2), '%')`.as('fallout_per'),
                    cancelled_per: sql<string>`CONCAT(ROUND(f.cancelled / f.total * 100.0, 2), '%')`.as('cancelled_per'),
                    total_re_non_ps: sql<number>`f.total`.as('total_re_non_ps'),

                    kendala_pelanggan: branchReNonPS.kendala_pelanggan,
                    kendala_sistem: branchReNonPS.kendala_sistem,
                    kendala_teknik: branchReNonPS.kendala_teknik,
                    kendala_others: branchReNonPS.kendala_others,
                    total_all_kendala: branchReNonPS.total_all_kendala,

                    INDIKASI_CABUT_PASANG: branchReNonPS.INDIKASI_CABUT_PASANG,
                    PENDING: branchReNonPS.PENDING,
                    PELANGGAN_MASIH_RAGU: branchReNonPS.PELANGGAN_MASIH_RAGU,
                    RNA: branchReNonPS.RNA,
                    KENDALA_IZIN: branchReNonPS.KENDALA_IZIN,
                    BATAL: branchReNonPS.BATAL,
                    RUMAH_KOSONG: branchReNonPS.RUMAH_KOSONG,
                    DOUBLE_INPUT: branchReNonPS.DOUBLE_INPUT,
                    GANTI_PAKET: branchReNonPS.GANTI_PAKET,
                    ALAMAT_TIDAK_DITEMUKAN: branchReNonPS.ALAMAT_TIDAK_DITEMUKAN,

                    SALAH_TAGGING: branchReNonPS.SALAH_TAGGING,
                    ODP_RUSAK: branchReNonPS.ODP_RUSAK,
                    ODP_FULL: branchReNonPS.ODP_FULL,
                    TIANG: branchReNonPS.TIANG,
                    CROSS_JALAN: branchReNonPS.CROSS_JALAN,
                    TIDAK_ADA_ODP: branchReNonPS.TIDAK_ADA_ODP,
                    ODP_JAUH: branchReNonPS.ODP_JAUH,
                    ODP_BELUM_GO_LIVE: branchReNonPS.ODP_BELUM_GO_LIVE,
                    ODP_RETI: branchReNonPS.ODP_RETI,
                    LIMITASI_ONU: branchReNonPS.LIMITASI_ONU,
                    JALUR_RUTE: branchReNonPS.JALUR_RUTE,
                    KENDALA_IKR_IKG: branchReNonPS.KENDALA_IKR_IKG,

                    wo_3: branchReNonPS.wo_3,
                    wo_3_per: branchReNonPS.wo_3_per,
                    wo_4_7: branchReNonPS.wo_4_7,
                    wo_4_7_per: branchReNonPS.wo_4_7_per,
                    wo_8_14: branchReNonPS.wo_8_14,
                    wo_8_14_per: branchReNonPS.wo_8_14_per,
                    wo_15_30: branchReNonPS.wo_15_30,
                    wo_15_30_per: branchReNonPS.wo_15_30_per,
                    wo_gt_30: branchReNonPS.wo_gt_30,
                    wo_gt_30_per: branchReNonPS.wo_gt_30_per,
                    total_wo: branchReNonPS.total_wo,

                    ioreps_event_date: sql<string>`${ioreps_branch.event_date}`.as('ioreps_event_date')
                })
                .from(branchTerritory)
                .leftJoin(summaryBranch, eq(branchTerritory.branch, summaryBranch.branch))
                .leftJoin(branchTargetRevenue, eq(summaryBranch.branch, branchTargetRevenue.branch))
                .leftJoin(branchPSByChannel, eq(branchTerritory.branch, branchPSByChannel.branch))
                .leftJoin(branchBrownGreen, eq(branchTerritory.branch, branchBrownGreen.branch))
                .leftJoin(branchReNonPS, eq(branchTerritory.branch, branchReNonPS.branch))
                .groupBy(branchTerritory.branch)

            const wokTerritory = db
                .select({
                    wok: territoryHousehold.wok,
                })
                .from(territoryHousehold)
                .where(
                    branch && wok ?
                        and(
                            eq(territoryHousehold.regional, 'MALUKU DAN PAPUA'),
                            eq(territoryHousehold.branch, branch),
                            eq(territoryHousehold.wok, wok)
                        ) :
                        branch ?
                            and(
                                eq(territoryHousehold.regional, 'MALUKU DAN PAPUA'),
                                eq(territoryHousehold.branch, branch)
                            ) :
                            eq(territoryHousehold.regional, 'MALUKU DAN PAPUA')
                )
                .groupBy(territoryHousehold.wok)
                .as('a')

            const [ioreps_wok] = await db
                .select({
                    event_date: sql<string>`COALESCE(
                        (SELECT event_date FROM ${summaryIoRePsWok} WHERE event_date = ${currDate} LIMIT 1),
                        (SELECT MAX(event_date) FROM ${summaryIoRePsWok})
                    )`,

                })
                .from(summaryIoRePsWok)
                .limit(1)

            const summaryWok = db
                .select()
                .from(summaryIoRePsWok)
                .where(and(
                    eq(summaryIoRePsWok.event_date, ioreps_wok.event_date),
                    eq(summaryIoRePsWok.channel, 'all'),
                    eq(summaryIoRePsWok.package, 'all'),
                    branch && wok ?
                        and(
                            eq(summaryIoRePsWok.branch, branch),
                            eq(summaryIoRePsWok.wok, wok)
                        ) :
                        branch ?
                            eq(summaryIoRePsWok.branch, branch) :
                            undefined
                ))
                .groupBy(summaryIoRePsWok.wok)
                .as('b')

            const wokTargetRevenue = db
                .select({
                    wok: territoryHousehold.wok,
                    target_all_sales: sum(targetHouseholdAll.all_sales).as('target_all_sales'),
                    target_grapari: sum(targetHouseholdAll.grapari).as('target_grapari'),
                    target_agency: sum(targetHouseholdAll.agency).as('target_agency'),
                    target_community: sum(targetHouseholdAll.community).as('target_community'),
                    target_digital: sum(targetHouseholdAll.digital).as('target_digital'),
                    target_greenfield: sum(targetHouseholdAll.greenfield).as('target_greenfield'),
                    target_brownfield: sum(targetHouseholdAll.brownfield).as('target_brownfield')
                })
                .from(targetHouseholdAll)
                .rightJoin(territoryHousehold, eq(targetHouseholdAll.territory, territoryHousehold.sto))
                .where(branch && wok ?
                    and(
                        eq(targetHouseholdAll.periode, yyyyMM),
                        eq(territoryHousehold.branch, branch),
                        eq(territoryHousehold.wok, wok)
                    ) :
                    branch ?
                        and(
                            eq(targetHouseholdAll.periode, yyyyMM),
                            eq(territoryHousehold.branch, branch)
                        ) :
                        eq(targetHouseholdAll.periode, yyyyMM))
                .groupBy(territoryHousehold.wok)
                .as('c')

            const wokPSByChannel = db
                .select({
                    wok: ihOrderingDetailOrder.wok,
                    grapari: sql<number>`ROUND(SUM(CASE WHEN order_channel IN ('k4', 'k3') THEN 1 ELSE 0 END) * 100.0 / COUNT(order_id), 2)`.as('grapari'),
                    total_grapari: sql<number>`COUNT(CASE WHEN order_channel IN ('k4', 'k3') THEN order_id END)`.as('total_grapari'),

                    indihome: sql<number>`ROUND(SUM(CASE WHEN order_channel IN ('b0') THEN 1 ELSE 0 END) * 100.0 / COUNT(order_id), 2)`.as('indihome'),
                    total_digital: sql<number>`COUNT(CASE WHEN order_channel IN ('b0') THEN order_id END)`.as('total_digital'),

                    lp_others: sql<number>`ROUND(SUM(CASE WHEN order_channel IN ('i1', 'a0', 's3', 'b3', 'u2', 'ab') THEN 1 ELSE 0 END) * 100.0 / COUNT(order_id), 2)`.as('lp_others'),
                    total_community: sql<number>`COUNT(CASE WHEN order_channel IN ('i1', 'a0', 's3', 'b3', 'u2', 'ab') THEN order_id END)`.as('total_community'),

                    sales_force: sql<number>`ROUND(SUM(CASE WHEN order_channel IN ('i4') THEN 1 ELSE 0 END) * 100.0 / COUNT(order_id), 2)`.as('sales_force'),
                    total_agency: sql<number>`COUNT(CASE WHEN order_channel IN ('i4') THEN order_id END)`.as('total_agency')
                })
                .from(ihOrderingDetailOrder)
                .where(and(
                    eq(ihOrderingDetailOrder.region, 'MALUKU DAN PAPUA'),
                    eq(ihOrderingDetailOrder.order_type, 'NEW SALES'),
                    inArray(ihOrderingDetailOrder.funneling_group, ['COMPLETED', 'PS']),
                    sql`DATE_FORMAT(${ihOrderingDetailOrder.ps_ts}, '%Y-%m-%d') BETWEEN ${currStartOfMonth} AND ${currDate}`
                ))
                .groupBy(sql`1`)
                .as('d')

            const wokBrownGreen = db
                .select({
                    wok: ihOrderingDetailOrder.wok,
                    greenfield: sql<number>`COUNT(CASE WHEN (${ih_occ_golive_ihld.odp_name} IS NOT NULL AND ${ih_occ_golive_ihld.tahun_program} = 2025 AND YEAR(${ih_occ_golive_ihld.tanggal_go_live_uim}) = 2025) THEN ${ihOrderingDetailOrder.order_id} END)`.as('greenfield'),
                    brownfield: sql<number>`COUNT(CASE WHEN (${ih_occ_golive_ihld.odp_name} IS NOT NULL AND ${ih_occ_golive_ihld.tahun_program} = 2024 AND YEAR(${ih_occ_golive_ihld.tanggal_go_live_uim}) = 2024) THEN ${ihOrderingDetailOrder.order_id} END)`.as('brownfield'),
                })
                .from(ihOrderingDetailOrder)
                .leftJoin(ih_information_odp, eq(ihOrderingDetailOrder.service_id, ih_information_odp.service_number))
                .leftJoin(ih_occ_golive_ihld, eq(ih_information_odp.odp_name, ih_occ_golive_ihld.odp_name))
                .where(and(
                    eq(ih_occ_golive_ihld.telkomsel_regional, 'MALUKU DAN PAPUA'),
                    isNotNull(ihOrderingDetailOrder.ps_ts),
                    sql`DATE_FORMAT(${ihOrderingDetailOrder.ps_ts}, '%Y-%m-%d') BETWEEN ${currStartOfMonth} AND ${currDate}`
                ))
                .groupBy(sql`1`)
                .as('e')

            const wokDeduplicatedData = db
                .select({
                    ...getTableColumns(ihOrderingDetailOrder),
                    duplicate_count: sql<number>`COUNT(*) OVER (PARTITION BY service_id)`.as('duplicate_count'),
                    completed_flag: sql<number>`SUM(CASE WHEN funneling_group IN ('COMPLETED', 'PS', 'CANCELLED') THEN 1 ELSE 0 END) OVER (PARTITION BY service_id)`.as('completed_flag')
                })
                .from(ihOrderingDetailOrder)
                .where(and(
                    eq(ihOrderingDetailOrder.region, 'MALUKU DAN PAPUA'),
                    eq(ihOrderingDetailOrder.order_type, 'NEW SALES'),
                    sql`DATE_FORMAT(${ihOrderingDetailOrder.re_ts}, '%Y-%m-%d') BETWEEN ${prevStartOfMonth} AND ${currDate}`,
                ))
                .as('deduplicated_data')

            const wokFinalDeduplicatedData = db
                .select()
                .from(wokDeduplicatedData)
                .where(and(
                    eq(wokDeduplicatedData.duplicate_count, 1),
                    eq(wokDeduplicatedData.completed_flag, 0),
                    notInArray(wokDeduplicatedData.funneling_group, ['COMPLETED', 'PS', 'CANCELLED'])
                ))
                .as('final_deduplicated')

            const wokReNonPS = db
                .select({
                    wok: wokFinalDeduplicatedData.wok,
                    registration: sql<number>`COUNT(CASE WHEN funneling_group = 'REGISTRATION' THEN order_id END)`.as('registration'),
                    provision_issued: sql<number>`COUNT(CASE WHEN funneling_group = 'PROVISION_ISSUED' THEN order_id END)`.as('provision_issued'),
                    provision_completed: sql<number>`COUNT(CASE WHEN funneling_group = 'PROVISION_COMPLETED' THEN order_id END)`.as('provision_completed'),
                    activation_completed: sql<number>`COUNT(CASE WHEN funneling_group = 'ACTIVATION_COMPLETED' THEN order_id END)`.as('activation_completed'),
                    fallout: sql<number>`COUNT(CASE WHEN funneling_group = 'FALLOUT' THEN order_id END)`.as('fallout'),
                    cancelled: sql<number>`COUNT(CASE WHEN funneling_group = 'CANCELLED' THEN order_id END)`.as('cancelled'),
                    total: sql<number>`COUNT(CASE WHEN funneling_group IN ('REGISTRATION', 'PROVISION_ISSUED', 'PROVISION_COMPLETED', 'ACTIVATION_COMPLETED', 'FALLOUT', 'CANCELLED') THEN order_id END)`.as('total'),

                    kendala_pelanggan: sql<number>`COUNT(CASE WHEN funneling_group = 'FALLOUT' AND fallout_category = 'KENDALA PELANGGAN' THEN order_id END)`.as('kendala_pelanggan'),
                    kendala_sistem: sql<number>`COUNT(CASE WHEN funneling_group = 'FALLOUT' AND fallout_category = 'KENDALA SISTEM' THEN order_id END)`.as('kendala_sistem'),
                    kendala_teknik: sql<number>`COUNT(CASE WHEN funneling_group = 'FALLOUT' AND fallout_category = 'KENDALA TEKNIK' THEN order_id END)`.as('kendala_teknik'),
                    kendala_others: sql<number>`COUNT(CASE WHEN funneling_group = 'FALLOUT' AND fallout_category IN ('OTHERS', 'KENDALA LAINNYA') THEN order_id END)`.as('kendala_others'),
                    total_all_kendala: sql<number>`COUNT(CASE WHEN funneling_group = 'FALLOUT' AND fallout_category IN ('KENDALA PELANGGAN', 'KENDALA TEKNIK', 'KENDALA SISTEM', 'OTHERS', 'KENDALA LAINNYA') THEN order_id END)`.as('total_all_kendala'),

                    // KENDALA PELANGGAN
                    INDIKASI_CABUT_PASANG: sql<number>`COUNT(CASE WHEN fallout_category = 'KENDALA PELANGGAN' AND fallout_subcategory = 'INDIKASI CABUT PASANG' THEN order_id END)`.as('INDIKASI_CABUT_PASANG'),
                    PENDING: sql<number>`COUNT(CASE WHEN fallout_category = 'KENDALA PELANGGAN' AND fallout_subcategory = 'PENDING' THEN order_id END)`.as('PENDING'),
                    PELANGGAN_MASIH_RAGU: sql<number>`COUNT(CASE WHEN fallout_category = 'KENDALA PELANGGAN' AND fallout_subcategory = 'PELANGGAN MASIH RAGU' THEN order_id END)`.as('PELANGGAN_MASIH_RAGU'),
                    RNA: sql<number>`COUNT(CASE WHEN fallout_category = 'KENDALA PELANGGAN' AND fallout_subcategory = 'RNA' THEN order_id END)`.as('RNA'),
                    KENDALA_IZIN: sql<number>`COUNT(CASE WHEN fallout_category = 'KENDALA PELANGGAN' AND fallout_subcategory = 'KENDALA IZIN' THEN order_id END)`.as('KENDALA_IZIN'),
                    BATAL: sql<number>`COUNT(CASE WHEN fallout_category = 'KENDALA PELANGGAN' AND fallout_subcategory = 'BATAL' THEN order_id END)`.as('BATAL'),
                    RUMAH_KOSONG: sql<number>`COUNT(CASE WHEN fallout_category = 'KENDALA PELANGGAN' AND fallout_subcategory = 'RUMAH KOSONG' THEN order_id END)`.as('RUMAH_KOSONG'),
                    DOUBLE_INPUT: sql<number>`COUNT(CASE WHEN fallout_category = 'KENDALA PELANGGAN' AND fallout_subcategory = 'DOUBLE INPUT' THEN order_id END)`.as('DOUBLE_INPUT'),
                    GANTI_PAKET: sql<number>`COUNT(CASE WHEN fallout_category = 'KENDALA PELANGGAN' AND fallout_subcategory = 'GANTI PAKET' THEN order_id END)`.as('GANTI_PAKET'),
                    ALAMAT_TIDAK_DITEMUKAN: sql<number>`COUNT(CASE WHEN fallout_category = 'KENDALA PELANGGAN' AND fallout_subcategory = 'ALAMAT TIDAK DITEMUKAN' THEN order_id END)`.as('ALAMAT_TIDAK_DITEMUKAN'),

                    // KENDALA TEKNIK
                    SALAH_TAGGING: sql<number>`COUNT(CASE WHEN fallout_category = 'KENDALA TEKNIK' AND fallout_subcategory = 'SALAH TAGGING' THEN order_id END)`.as('SALAH_TAGGING'),
                    ODP_RUSAK: sql<number>`COUNT(CASE WHEN fallout_category = 'KENDALA TEKNIK' AND fallout_subcategory = 'ODP RUSAK' THEN order_id END)`.as("ODP_RUSAK"),
                    ODP_FULL: sql<number>`COUNT(CASE WHEN fallout_category = 'KENDALA TEKNIK' AND fallout_subcategory = 'ODP FULL' THEN order_id END)`.as("ODP_FULL"),
                    TIANG: sql<number>`COUNT(CASE WHEN fallout_category = 'KENDALA TEKNIK' AND fallout_subcategory = 'TIANG' THEN order_id END)`.as("TIANG"),
                    CROSS_JALAN: sql<number>`COUNT(CASE WHEN fallout_category = 'KENDALA TEKNIK' AND fallout_subcategory = 'CROSS JALAN' THEN order_id END)`.as("CROSS_JALAN"),
                    TIDAK_ADA_ODP: sql<number>`COUNT(CASE WHEN fallout_category = 'KENDALA TEKNIK' AND fallout_subcategory = 'TIDAK ADA ODP' THEN order_id END)`.as("TIDAK_ADA_ODP"),
                    ODP_JAUH: sql<number>`COUNT(CASE WHEN fallout_category = 'KENDALA TEKNIK' AND fallout_subcategory = 'ODP JAUH' THEN order_id END)`.as("ODP_JAUH"),
                    ODP_BELUM_GO_LIVE: sql<number>`COUNT(CASE WHEN fallout_category = 'KENDALA TEKNIK' AND fallout_subcategory = 'ODP BELUM GO LIVE' THEN order_id END)`.as('ODP_BELUM_GO_LIVE'),
                    ODP_RETI: sql<number>`COUNT(CASE WHEN fallout_category = 'KENDALA TEKNIK' AND fallout_subcategory = 'ODP RETI' THEN order_id END)`.as('ODP_RETI'),
                    LIMITASI_ONU: sql<number>`COUNT(CASE WHEN fallout_category = 'KENDALA TEKNIK' AND fallout_subcategory = 'LIMITASI ONU' THEN order_id END)`.as('LIMITASI_ONU'),
                    JALUR_RUTE: sql<number>`COUNT(CASE WHEN fallout_category = 'KENDALA TEKNIK' AND fallout_subcategory IN ('KENDALA JALUR/RUTE TARIKAN', 'KENDALA JALUR/RUTE', 'KENDALA JALUR') THEN order_id END)`.as('JALUR_RUTE'),
                    KENDALA_IKR_IKG: sql<number>`COUNT(CASE WHEN fallout_category = 'KENDALA TEKNIK' AND fallout_subcategory = 'KENDALA IKR/IKG' THEN order_id END)`.as('KENDALA_IKR_IKG'),

                    // -LoS
                    wo_3: sql<number>`COUNT(CASE WHEN FLOOR(provi_duration) BETWEEN 0 AND 3 THEN order_id END)`.as('wo_3'),
                    wo_3_per: sql<string>`CONCAT(ROUND(SUM(CASE WHEN FLOOR(provi_duration) BETWEEN 0 AND 3 THEN 1 ELSE 0 END) * 100 / COUNT(order_id), 2), '%')`.as('wo_3_per'),
                    wo_4_7: sql<number>`COUNT(CASE WHEN FLOOR(provi_duration) BETWEEN 4 AND 7 THEN order_id END)`.as('wo_4_7'),
                    wo_4_7_per: sql<string>`CONCAT(ROUND(SUM(CASE WHEN FLOOR(provi_duration) BETWEEN 4 AND 7 THEN 1 ELSE 0 END) * 100 / COUNT(order_id), 2), '%')`.as('wo_4_7_per'),
                    wo_8_14: sql<number>`COUNT(CASE WHEN FLOOR(provi_duration) BETWEEN 8 AND 14 THEN order_id END)`.as('wo_8_14'),
                    wo_8_14_per: sql<string>`CONCAT(ROUND(SUM(CASE WHEN FLOOR(provi_duration) BETWEEN 8 AND 14 THEN 1 ELSE 0 END) * 100 / COUNT(order_id), 2), '%')`.as('wo_8_14_per'),
                    wo_15_30: sql<number>`COUNT(CASE WHEN FLOOR(provi_duration) BETWEEN 15 AND 30 THEN order_id END)`.as('wo_15_30'),
                    wo_15_30_per: sql<string>`CONCAT(ROUND(SUM(CASE WHEN FLOOR(provi_duration) BETWEEN 15 AND 30 THEN 1 ELSE 0 END) * 100 / COUNT(order_id), 2), '%')`.as('wo_15_30_per'),
                    wo_gt_30: sql<number>`COUNT(CASE WHEN FLOOR(provi_duration) > 30 THEN order_id END)`.as('wo_gt_30'),
                    wo_gt_30_per: sql<string>`CONCAT(ROUND(SUM(CASE WHEN FLOOR(provi_duration) > 30 THEN 1 ELSE 0 END) * 100 / COUNT(order_id), 2), '%')`.as('wo_gt_30_per'),
                    total_wo: sql<number>`COUNT(CASE WHEN provi_duration IS NOT NULL THEN order_id END)`.as('total_wo')
                })
                .from(wokFinalDeduplicatedData)
                .groupBy(sql`1`)
                .as('f')

            const revenueWok = db
                .select({
                    name: wokTerritory.wok,
                    target_all_sales: sql<number>`ROUND(SUM(${wokTargetRevenue.target_all_sales}),2)`.as('target_all_sales'),
                    level: sql<string>`'wok'`.as('level'),

                    ach_target_fm_io_all_sales: sql<number>`ROUND((SUM(${summaryWok.io_m})/SUM(${wokTargetRevenue.target_all_sales}))*100, 2)`.as('ach_target_fm_io_all_sales'),
                    drr_io: sql<number>`ROUND((SUM(${summaryWok.io_m})/(${today}/${daysInMonth}*SUM(${wokTargetRevenue.target_all_sales})))*100, 2)`.as('drr_io'),
                    io_m: sql<number>`ROUND(SUM(${summaryWok.io_m}),2)`.as('io_m'),
                    io_mom: sql<string>`CONCAT(${summaryWok.io_mom}, '%')`.as('io_mom'),
                    io_gap_daily: sql<number>`ROUND(SUM(${summaryWok.io_gap_daily}),2)`.as('io_gap_daily'),

                    ach_target_fm_re_all_sales: sql<number>`ROUND((SUM(${summaryWok.re_m})/SUM(${wokTargetRevenue.target_all_sales}))*100, 2)`.as('ach_target_fm_re_all_sales'),
                    drr_re: sql<number>`ROUND((SUM(${summaryWok.re_m})/(${today}/${daysInMonth}*SUM(${wokTargetRevenue.target_all_sales})))*100, 2)`.as('drr_re'),
                    re_m: sql<number>`ROUND(SUM(${summaryWok.re_m}),2)`.as('re_m'),
                    re_mom: sql<string>`CONCAT(${summaryWok.re_mom}, '%')`.as('re_mom'),
                    re_gap_daily: sql<number>`ROUND(SUM(${summaryWok.re_gap_daily}),2)`.as('re_gap_daily'),

                    ach_target_fm_ps_all_sales: sql<number>`ROUND((SUM(${summaryWok.ps_m})/SUM(${wokTargetRevenue.target_all_sales}))*100, 2)`.as('ach_target_fm_ps_all_sales'),
                    drr_ps: sql<number>`ROUND((SUM(${summaryWok.ps_m})/(${today}/${daysInMonth}*SUM(${wokTargetRevenue.target_all_sales})))*100, 2)`.as('drr_ps'),
                    target_daily_ps: sql<number>`ROUND(SUM(${wokTargetRevenue.target_all_sales}/${daysInMonth}), 2)`.as('target_daily_ps'),
                    ach_daily_ps: sql<number>`ROUND(SUM(${summaryWok.ps_m}/${today}), 2)`.as('ach_daily_ps'),
                    ps_gap_daily: sql<number>`ROUND(SUM(${summaryWok.ps_gap_daily}), 2)`.as('ps_gap_daily'),
                    daily_ps_remaining: sql<number>`ROUND(SUM(${wokTargetRevenue.target_all_sales} - ${summaryWok.ps_m})/${remainingDays},2)`.as('daily_ps_remaining'),
                    ps_m: sql<number>`ROUND(SUM(${summaryWok.ps_m}),2)`.as('ps_m'),
                    ps_mom: sql<string>`CONCAT(${summaryWok.ps_mom}, '%')`.as('ps_mom'),
                    ps_to_io: sql<number>`CONCAT(${summaryWok.ps_to_io}, '%')`.as('ps_to_io'),
                    ps_to_re: sql<number>`CONCAT(${summaryWok.ps_to_re}, '%')`.as('ps_to_re'),

                    ach_fm_indihome: sql<string>`CONCAT(d.indihome,'%')`.as('ach_fm_indihome'),
                    ps_indihome: sql<number>`ROUND(d.total_digital, 2)`.as('ps_indihome'),
                    ach_fm_grapari: sql<string>`CONCAT(d.grapari,'%')`.as('ach_fm_grapari'),
                    ps_grapari: sql<number>`ROUND(d.total_grapari, 2)`.as('ps_grapari'),
                    ach_fm_community: sql<string>`CONCAT(d.lp_others,'%')`.as('ach_fm_community'),
                    ps_community: sql<number>`ROUND(d.total_community, 2)`.as('ps_community'),
                    ach_fm_agency: sql<string>`CONCAT(d.sales_force,'%')`.as('ach_fm_agency'),
                    ps_sales_force: sql<number>`ROUND(d.total_agency, 2)`.as('ps_agency'),

                    brownfield: sql<number>`ROUND(SUM(e.brownfield), 2)`.as('brownfield'),
                    target_brownfield: sql<number>`ROUND(SUM(c.target_brownfield),2)`.as('target_brownfield'),
                    ach_fm_brownfield: sql<string>`CONCAT(ROUND((SUM(e.brownfield)/SUM(c.target_brownfield))*100, 2), '%')`.as('ach_fm_brownfield'),
                    drr_brownfield: sql<string>`CONCAT(ROUND((SUM(e.brownfield)/(${today}/${daysInMonth}*SUM(c.target_brownfield)))*100, 2), '%')`.as('drr_brownfield'),
                    greenfield: sql<number>`ROUND(SUM(e.greenfield), 2)`.as('greenfield'),
                    target_greenfield: sql<number>`ROUND(SUM(c.target_greenfield),2)`.as('target_greenfield'),
                    ach_fm_greenfield: sql<string>`CONCAT(ROUND((SUM(e.greenfield)/SUM(c.target_greenfield))*100, 2), '%')`.as('ach_fm_greenfield'),
                    drr_greenfield: sql<string>`CONCAT(ROUND((SUM(e.greenfield)/(${today}/${daysInMonth}*SUM(c.target_greenfield)))*100, 2), '%')`.as('drr_greenfield'),

                    registration: branchReNonPS.registration,
                    provision_issued: branchReNonPS.provision_issued,
                    provision_completed: branchReNonPS.provision_completed,
                    activation_completed: branchReNonPS.activation_completed,
                    fallout: branchReNonPS.fallout,
                    cancelled: branchReNonPS.cancelled,
                    registration_per: sql<string>`CONCAT(ROUND(f.registration / f.total * 100.0, 2),'%')`.as('registration_per'),
                    provision_issued_per: sql<string>`CONCAT(ROUND(f.provision_issued / f.total * 100.0, 2), '%')`.as('provision_issued_per'),
                    provision_completed_per: sql<string>`CONCAT(ROUND(f.provision_completed / f.total * 100.0, 2), '%')`.as('provision_completed_per'),
                    activation_completed_per: sql<string>`CONCAT(ROUND(f.activation_completed / f.total * 100.0, 2), '%')`.as('activation_completed_per'),
                    fallout_per: sql<string>`CONCAT(ROUND(f.fallout / f.total * 100.0, 2), '%')`.as('fallout_per'),
                    cancelled_per: sql<string>`CONCAT(ROUND(f.cancelled / f.total * 100.0, 2), '%')`.as('cancelled_per'),
                    total_re_non_ps: sql<number>`f.total`.as('total_re_non_ps'),

                    kendala_pelanggan: branchReNonPS.kendala_pelanggan,
                    kendala_sistem: branchReNonPS.kendala_sistem,
                    kendala_teknik: branchReNonPS.kendala_teknik,
                    kendala_others: branchReNonPS.kendala_others,
                    total_all_kendala: branchReNonPS.total_all_kendala,

                    INDIKASI_CABUT_PASANG: wokReNonPS.INDIKASI_CABUT_PASANG,
                    PENDING: wokReNonPS.PENDING,
                    PELANGGAN_MASIH_RAGU: wokReNonPS.PELANGGAN_MASIH_RAGU,
                    RNA: wokReNonPS.RNA,
                    KENDALA_IZIN: wokReNonPS.KENDALA_IZIN,
                    BATAL: wokReNonPS.BATAL,
                    RUMAH_KOSONG: wokReNonPS.RUMAH_KOSONG,
                    DOUBLE_INPUT: wokReNonPS.DOUBLE_INPUT,
                    GANTI_PAKET: wokReNonPS.GANTI_PAKET,
                    ALAMAT_TIDAK_DITEMUKAN: wokReNonPS.ALAMAT_TIDAK_DITEMUKAN,

                    SALAH_TAGGING: wokReNonPS.SALAH_TAGGING,
                    ODP_RUSAK: wokReNonPS.ODP_RUSAK,
                    ODP_FULL: wokReNonPS.ODP_FULL,
                    TIANG: wokReNonPS.TIANG,
                    CROSS_JALAN: wokReNonPS.CROSS_JALAN,
                    TIDAK_ADA_ODP: wokReNonPS.TIDAK_ADA_ODP,
                    ODP_JAUH: wokReNonPS.ODP_JAUH,
                    ODP_BELUM_GO_LIVE: wokReNonPS.ODP_BELUM_GO_LIVE,
                    ODP_RETI: wokReNonPS.ODP_RETI,
                    LIMITASI_ONU: wokReNonPS.LIMITASI_ONU,
                    JALUR_RUTE: wokReNonPS.JALUR_RUTE,
                    KENDALA_IKR_IKG: wokReNonPS.KENDALA_IKR_IKG,

                    wo_3: wokReNonPS.wo_3,
                    wo_3_per: wokReNonPS.wo_3_per,
                    wo_4_7: wokReNonPS.wo_4_7,
                    wo_4_7_per: wokReNonPS.wo_4_7_per,
                    wo_8_14: wokReNonPS.wo_8_14,
                    wo_8_14_per: wokReNonPS.wo_8_14_per,
                    wo_15_30: wokReNonPS.wo_15_30,
                    wo_15_30_per: wokReNonPS.wo_15_30_per,
                    wo_gt_30: wokReNonPS.wo_gt_30,
                    wo_gt_30_per: wokReNonPS.wo_gt_30_per,
                    total_wo: branchReNonPS.total_wo,

                    ioreps_event_date: sql<string>`${ioreps_wok.event_date}`.as('ioreps_event_date')
                })
                .from(wokTerritory)
                .leftJoin(summaryWok, eq(wokTerritory.wok, summaryWok.wok))
                .leftJoin(wokTargetRevenue, eq(summaryWok.wok, wokTargetRevenue.wok))
                .leftJoin(wokPSByChannel, eq(wokTerritory.wok, wokPSByChannel.wok))
                .leftJoin(wokBrownGreen, eq(wokTerritory.wok, wokBrownGreen.wok))
                .leftJoin(wokReNonPS, eq(wokTerritory.wok, wokReNonPS.wok))
                .groupBy(wokTerritory.wok)


            if (branch && wok) {
                const [finalDataRevenue] = await Promise.all([
                    revenueWok
                ])

                return c.json({ data: finalDataRevenue })
            }

            if (branch) {
                const [finalDataRevenue] = await Promise.all([
                    revenueBranch
                ])

                return c.json({ data: finalDataRevenue })
            }

            const [finalDataRevenue] = await Promise.all([
                revenueRegional
            ])

            return c.json({ data: finalDataRevenue })
        })
    .get('/demands-deployment', zValidator('query', z.object({ date: z.string().optional(), branch: z.string().optional(), wok: z.string().optional() })),
        async c => {
            const { date, branch, wok } = c.req.valid('query')
            const selectedDate = date ? parseISO(date) : subDays(new Date(), 2)

            const currMonth = format(selectedDate, 'MM')
            const currYear = format(selectedDate, 'yyyy')
            const latestMonth = parseInt(format(selectedDate, 'M'), 10)
            const isPrevMonthLastYear = currMonth === '01'
            const prevMonth = isPrevMonthLastYear ? '12' : format(subMonths(selectedDate, 1), 'MM')
            const prevMonthYear = isPrevMonthLastYear ? format(subYears(selectedDate, 1), 'yyyy') : format(selectedDate, 'yyyy')
            const prevYear = format(subYears(selectedDate, 1), 'yyyy')

            const currOdpTable = dynamicIhAvailOdpA4(currYear, currMonth)
            const prevOdpTable = dynamicIhAvailOdpA4(prevMonthYear, prevMonth)

            const lastDayOfSelectedMonth = endOfMonth(selectedDate);
            const isEndOfMonth = selectedDate.getDate() === lastDayOfSelectedMonth.getDate();

            const endOfCurrMonth = isEndOfMonth ? lastDayOfSelectedMonth : selectedDate;
            const endOfPrevMonth = endOfMonth(subMonths(selectedDate, 1))
            const endOfPrevMonth2 = isEndOfMonth ? endOfMonth(subMonths(selectedDate, 1)) : subMonths(selectedDate, 1)
            const endOfPrevYearSameMonth = isEndOfMonth ? endOfMonth(subYears(selectedDate, 1)) : subYears(selectedDate, 1);

            const currStartOfMonth = format(startOfMonth(endOfCurrMonth), 'yyyy-MM-dd')
            const currDate = format(endOfCurrMonth, 'yyyy-MM-dd')
            const prevStartOfMonth = format(startOfMonth(endOfPrevMonth2), 'yyyy-MM-dd')
            const prevMonthSameDate = format(endOfPrevMonth2, 'yyyy-MM-dd')
            const prevDate = format(endOfPrevMonth, 'yyyy-MM-dd')
            const prevDate2 = format(endOfPrevMonth2, 'yyyy-MM-dd')
            const prevYearCurrDate = format(endOfPrevYearSameMonth, 'yyyy-MM-dd')

            const daysInMonth = getDaysInMonth(selectedDate)
            const today = Number(format(selectedDate, 'd'))

            const currJanuaryFirst = `${currYear}-01-01`
            const prevJanuaryFirst = `${prevYear}-01-01`

            const regionalTerritory = db
                .select({ regional: territoryHousehold.regional })
                .from(territoryHousehold)
                .where(eq(territoryHousehold.regional, 'MALUKU DAN PAPUA'))
                .groupBy(territoryHousehold.regional)
                .as('a')

            const regionalCurrOdp = db
                .select({
                    regional: sql<string>`bb.regional`.as('regional'),
                    used_black: sql<number>`SUM(CASE WHEN status = 'BLACK' THEN used END)`.as('used_black'),
                    used_red: sql<number>`SUM(CASE WHEN status = 'RED' THEN used END)`.as('used_red'),
                    used_yellow: sql<number>`SUM(CASE WHEN status = 'YELLOW' THEN used END)`.as('used_yellow'),
                    used_green: sql<number>`SUM(CASE WHEN status = 'GREEN' THEN used END)`.as('used_green'),
                    used: sql<number>`SUM(used)`.as('used'),

                    avai_port_black: sql<number>`SUM(CASE WHEN status = 'BLACK' THEN avai_port END)`.as('avai_port_black'),
                    avai_port_red: sql<number>`SUM(CASE WHEN status = 'RED' THEN avai_port END)`.as('avai_port_red'),
                    avai_port_yellow: sql<number>`SUM(CASE WHEN status = 'YELLOW' THEN avai_port END)`.as('avai_port_yellow'),
                    avai_port_green: sql<number>`SUM(CASE WHEN status = 'GREEN' THEN avai_port END)`.as('avai_port_green'),
                    avai_port: sql<number>`SUM(avai_port)`.as('avai_port'),

                    amount_port_black: sql<number>`SUM(CASE WHEN status = 'BLACK' THEN amount_port END)`.as('amount_port_black'),
                    amount_port_red: sql<number>`SUM(CASE WHEN status = 'RED' THEN amount_port END)`.as('amount_port_red'),
                    amount_port_yellow: sql<number>`SUM(CASE WHEN status = 'YELLOW' THEN amount_port END)`.as('amount_port_yellow'),
                    amount_port_green: sql<number>`SUM(CASE WHEN status = 'GREEN' THEN amount_port END)`.as('amount_port_green'),
                    amount_port: sql<number>`SUM(amount_port)`.as('amount_port'),

                    total_odp_black: sql<number>`SUM(CASE WHEN status = 'BLACK' THEN odp END)`.as('total_odp_black'),
                    total_odp_red: sql<number>`SUM(CASE WHEN status = 'RED' THEN odp END)`.as('total_odp_red'),
                    total_odp_yellow: sql<number>`SUM(CASE WHEN status = 'YELLOW' THEN odp END)`.as('total_odp_yellow'),
                    total_odp_green: sql<number>`SUM(CASE WHEN status = 'GREEN' THEN odp END)`.as('total_odp_green'),
                    total_odp: sql<number>`SUM(bb.odp)`.as('total_odp')
                })
                .from(db
                    .select({
                        regional: sql<string>`'MALUKU DAN PAPUA'`.as('regional'),
                        status: sql<string>`
                        CASE
                            WHEN ${currOdpTable.status} IN ('BLACK', 'BLACKSYSTEM') THEN 'BLACK' ELSE ${currOdpTable.status}
                        END`.as('status'),
                        amount_port: sum(currOdpTable.is_total).as('amount_port'),
                        avai_port: sum(currOdpTable.avai).as('avai_port'),
                        used: sum(currOdpTable.used).as('used'),
                        odp: count(currOdpTable.status).as('odp')
                    })
                    .from(currOdpTable)
                    .where(inArray(currOdpTable.witel, ['MALUKU', 'PAPUA', 'PAPUA BARAT']))
                    .groupBy(sql`1,2`)
                    .as('bb')
                )
                .groupBy(sql`1`)
                .as('b')

            const regionalPrevOdp = db
                .select({
                    regional: sql<string>`cc.regional`.as('regional'),
                    used_black: sql<number>`SUM(CASE WHEN status = 'BLACK' THEN used END)`.as('used_black'),
                    used_red: sql<number>`SUM(CASE WHEN status = 'RED' THEN used END)`.as('used_red'),
                    used_yellow: sql<number>`SUM(CASE WHEN status = 'YELLOW' THEN used END)`.as('used_yellow'),
                    used_green: sql<number>`SUM(CASE WHEN status = 'GREEN' THEN used END)`.as('used_green'),
                    used: sql<number>`SUM(used)`.as('used'),

                    amount_port_black: sql<number>`SUM(CASE WHEN status = 'BLACK' THEN amount_port END)`.as('amount_port_black'),
                    amount_port_red: sql<number>`SUM(CASE WHEN status = 'RED' THEN amount_port END)`.as('amount_port_red'),
                    amount_port_yellow: sql<number>`SUM(CASE WHEN status = 'YELLOW' THEN amount_port END)`.as('amount_port_yellow'),
                    amount_port_green: sql<number>`SUM(CASE WHEN status = 'GREEN' THEN amount_port END)`.as('amount_port_green'),
                    amount_port: sql<number>`SUM(amount_port)`.as('amount_port'),
                })
                .from(db
                    .select({
                        regional: sql<string>`'MALUKU DAN PAPUA'`.as('regional'),
                        status: sql<string>`
                    CASE
                        WHEN ${prevOdpTable.status} IN ('BLACK', 'BLACKSYSTEM') THEN 'BLACK' ELSE ${prevOdpTable.status}
                    END`.as('status'),
                        used: sum(prevOdpTable.used).as('used'),
                        amount_port: sum(prevOdpTable.is_total).as('amount_port')
                    })
                    .from(prevOdpTable)
                    .where(inArray(prevOdpTable.witel, ['MALUKU', 'PAPUA', 'PAPUA BARAT']))
                    .groupBy(sql`1,2`)
                    .as('cc')
                )
                .groupBy(sql`1`)
                .as('c');

            const regionalGoLive = db
                .select({
                    regional: sql<string>`CASE WHEN telkomsel_regional IN ('PUMA', 'MALUKU DAN PAPUA') THEN 'MALUKU DAN PAPUA' END`.as('regional'),
                    golive_m: sql<number>`COALESCE(COUNT(CASE WHEN ${ih_occ_golive_ihld.tanggal_go_live_uim} BETWEEN ${currStartOfMonth} AND ${currDate} THEN 1 END), 0)`.as('golive_m'),
                    golive_m1: sql<number>`COALESCE(COUNT(CASE WHEN ${ih_occ_golive_ihld.tanggal_go_live_uim} BETWEEN ${prevStartOfMonth} AND ${prevDate2} THEN 1 END), 0)`.as('golive_m1'),

                    // Add y and y1 calculations here directly
                    golive_y: sql<number>`COUNT(CASE WHEN tahun_go_live_uim = 2025 THEN 1 END)`.as('golive_y'),
                    golive_y1: sql<number>`COUNT(CASE WHEN tahun_go_live_uim = 2024 THEN 1 END)`.as('golive_y1'),

                    golive_ytd: sql<number>`COUNT(CASE WHEN ${ih_occ_golive_ihld.tanggal_go_live_uim} BETWEEN ${currJanuaryFirst} AND ${currDate} THEN 1 END)`.as('golive_ytd'),
                    golive_ytd1: sql<number>`COUNT(CASE WHEN ${ih_occ_golive_ihld.tanggal_go_live_uim} BETWEEN ${prevJanuaryFirst} AND ${prevYearCurrDate} THEN 1 END)`.as('golive_ytd1'),

                    // Calculate MoM and YoY percentages here as well
                    golive_mom: sql<string>`CONCAT(COALESCE(ROUND((COUNT(CASE WHEN ${ih_occ_golive_ihld.tanggal_go_live_uim} BETWEEN ${currStartOfMonth} AND ${currDate} THEN 1 END) - COUNT(CASE WHEN ${ih_occ_golive_ihld.tanggal_go_live_uim} BETWEEN ${prevStartOfMonth} AND ${prevDate2} THEN 1 END)) / COUNT(CASE WHEN ${ih_occ_golive_ihld.tanggal_go_live_uim} BETWEEN ${prevStartOfMonth} AND ${prevDate2} THEN 1 END) * 100, 2),0), '%')`.as('golive_mom'),
                    golive_yoy: sql<string>`CONCAT(COALESCE(ROUND((COUNT(CASE WHEN tahun_go_live_uim = 2025 THEN 1 END) - COUNT(CASE WHEN tahun_go_live_uim = 2024 THEN 1 END)) / COUNT(CASE WHEN tahun_go_live_uim = 2024 THEN 1 END) * 100, 2),0), '%')`.as('golive_yoy'),
                    golive_ytd_per: sql<string>`CONCAT(COALESCE(ROUND((COUNT(CASE WHEN ${ih_occ_golive_ihld.tanggal_go_live_uim} BETWEEN ${currJanuaryFirst} AND ${currDate} THEN 1 END) - COUNT(CASE WHEN ${ih_occ_golive_ihld.tanggal_go_live_uim} BETWEEN ${prevJanuaryFirst} AND ${prevYearCurrDate} THEN 1 END)) / COUNT(CASE WHEN ${ih_occ_golive_ihld.tanggal_go_live_uim} BETWEEN ${prevJanuaryFirst} AND ${prevYearCurrDate} THEN 1 END) * 100, 2),0), '%')`.as('golive_ytd_per'),
                })
                .from(ih_occ_golive_ihld)
                .groupBy(sql`1`)
                .as('d')

            const regionalDemands = db
                .select({
                    regional: ih_demand_mysiis.region,
                    demand_created_mtd: sql<number>`SUM(CASE WHEN DATE_FORMAT(approved_at, '%Y-%m-%d') BETWEEN ${currStartOfMonth} AND ${currDate} THEN ${ih_demand_mysiis.target} END)`.as('demand_created_mtd'),
                    demand_created_m1: sql<number>`SUM(CASE WHEN DATE_FORMAT(approved_at, '%Y-%m-%d') BETWEEN ${prevStartOfMonth} AND ${prevMonthSameDate} THEN ${ih_demand_mysiis.target} END)`.as('demand_created_m1'),
                })
                .from(ih_demand_mysiis)
                .where(and(
                    gt(ih_demand_mysiis.target, 0),
                    eq(ih_demand_mysiis.region, 'MALUKU DAN PAPUA'),
                    inArray(ih_demand_mysiis.status_new_bispro, ['Wait Approval AI', 'Draft']),
                    sql`YEAR(${ih_demand_mysiis.approved_at}) = ${currYear}`
                ))
                .groupBy(sql`1`)
                .as('e')

            const regionalGoLiveCurrYear = db
                .select({
                    regional: sql<string>`ff.telkomsel_regional`.as('regional'),

                    avai_port_1mo_y: sql<number>`COALESCE(ff.avai_port_1mo_y, 0)`.as('avai_port_1mo_y'),
                    avai_port_2mo_y: sql<number>`COALESCE(ff.avai_port_2mo_y, 0)`.as('avai_port_2mo_y'),
                    avai_port_3mo_y: sql<number>`COALESCE(ff.avai_port_3mo_y, 0)`.as('avai_port_3mo_y'),
                    avai_port_4mo_y: sql<number>`COALESCE(ff.avai_port_4mo_y, 0)`.as('avai_port_4mo_y'),

                    used_1mo_y: sql<number>`COALESCE(ff.used_1mo_y, 0)`.as('used_1mo_y'),
                    used_2mo_y: sql<number>`COALESCE(ff.used_2mo_y, 0)`.as('used_2mo_y'),
                    used_3mo_y: sql<number>`COALESCE(ff.used_3mo_y, 0)`.as('used_3mo_y'),
                    used_4mo_y: sql<number>`COALESCE(ff.used_4mo_y, 0)`.as('used_4mo_y'),
                    used_gt_6mo_y: sql<number>`COALESCE(ff.used_gt_6mo_y, 0)`.as('used_gt_6mo_y'),
                    used_all_mo_y: sql<number>`COALESCE(ff.used_all_mo_y, 0)`.as('used_all_mo_y'),

                    amount_port_1mo_y: sql<number>`COALESCE(ff.amount_port_1mo_y, 0)`.as('amount_port_1mo_y'),
                    amount_port_2mo_y: sql<number>`COALESCE(ff.amount_port_2mo_y, 0)`.as('amount_port_2mo_y'),
                    amount_port_3mo_y: sql<number>`COALESCE(ff.amount_port_3mo_y, 0)`.as('amount_port_3mo_y'),
                    amount_port_4mo_y: sql<number>`COALESCE(ff.amount_port_4mo_y, 0)`.as('amount_port_4mo_y'),
                    amount_port_gt_6mo_y: sql<number>`COALESCE(ff.amount_port_gt_6mo_y, 0)`.as('amount_port_gt_6mo_y'),
                    amount_port_all_mo_y: sql<number>`COALESCE(ff.amount_port_all_mo_y, 0)`.as('amount_port_all_mo_y'),

                    used_1mo_y1: sql<number>`COALESCE(ff.used_1mo_y1, 0)`.as('used_1mo_y1'),
                    used_2mo_y1: sql<number>`COALESCE(ff.used_2mo_y1, 0)`.as('used_2mo_y1'),
                    used_3mo_y1: sql<number>`COALESCE(ff.used_3mo_y1, 0)`.as('used_3mo_y1'),
                    used_4mo_y1: sql<number>`COALESCE(ff.used_4mo_y1, 0)`.as('used_4mo_y1'),
                    used_gt_6mo_y1: sql<number>`COALESCE(ff.used_gt_6mo_y1, 0)`.as('used_gt_6mo_y1'),
                    used_all_mo_y1: sql<number>`COALESCE(ff.used_all_mo_y1, 0)`.as('used_all_mo_y1'),

                    amount_port_1mo_y1: sql<number>`COALESCE(ff.amount_port_1mo_y1, 0)`.as('amount_port_1mo_y1'),
                    amount_port_2mo_y1: sql<number>`COALESCE(ff.amount_port_2mo_y1, 0)`.as('amount_port_2mo_y1'),
                    amount_port_3mo_y1: sql<number>`COALESCE(ff.amount_port_3mo_y1, 0)`.as('amount_port_3mo_y1'),
                    amount_port_4mo_y1: sql<number>`COALESCE(ff.amount_port_4mo_y1, 0)`.as('amount_port_4mo_y1'),
                    amount_port_gt_6mo_y1: sql<number>`COALESCE(ff.amount_port_gt_6mo_y1, 0)`.as('amount_port_gt_6mo_y1'),
                    amount_port_all_mo_y1: sql<number>`COALESCE(ff.amount_port_all_mo_y1, 0)`.as('amount_port_all_mo_y1'),

                    occ_1mo_y: sql<string>`CONCAT(COALESCE(ROUND(SUM(used_1mo_y) / SUM(amount_port_1mo_y) * 100, 2), 0), '%')`.as('occ_1mo_y'),
                    occ_2mo_y: sql<string>`CONCAT(COALESCE(ROUND(SUM(used_2mo_y) / SUM(amount_port_2mo_y) * 100, 2), 0), '%')`.as('occ_2mo_y'),
                    occ_3mo_y: sql<string>`CONCAT(COALESCE(ROUND(SUM(used_3mo_y) / SUM(amount_port_3mo_y) * 100, 2), 0), '%')`.as('occ_3mo_y'),
                    occ_4mo_y: sql<string>`CONCAT(COALESCE(ROUND(SUM(used_4mo_y) / SUM(amount_port_4mo_y) * 100, 2), 0), '%')`.as('occ_4mo_y'),
                    occ_gt_6mo_y: sql<string>`CONCAT(COALESCE(ROUND(SUM(used_gt_6mo_y) / SUM(amount_port_gt_6mo_y) * 100, 2), 0), '%')`.as('occ_gt_6mo_y'),
                    occ_all_mo_y: sql<string>`CONCAT(COALESCE(ROUND(SUM(used_all_mo_y) / SUM(amount_port_all_mo_y) * 100, 2), 0), '%')`.as('occ_all_mo_y'),

                    occ_1mo_y1: sql<string>`CONCAT(COALESCE(ROUND(SUM(used_1mo_y1) / SUM(amount_port_1mo_y1) * 100, 2), 0), '%')`.as('occ_1mo_y1'),
                    occ_2mo_y1: sql<string>`CONCAT(COALESCE(ROUND(SUM(used_2mo_y1) / SUM(amount_port_2mo_y1) * 100, 2), 0), '%')`.as('occ_2mo_y1'),
                    occ_3mo_y1: sql<string>`CONCAT(COALESCE(ROUND(SUM(used_3mo_y1) / SUM(amount_port_3mo_y1) * 100, 2), 0), '%')`.as('occ_3mo_y1'),
                    occ_4mo_y1: sql<string>`CONCAT(COALESCE(ROUND(SUM(used_4mo_y1) / SUM(amount_port_4mo_y1) * 100, 2), 0), '%')`.as('occ_4mo_y1'),
                    occ_gt_6mo_y1: sql<string>`CONCAT(COALESCE(ROUND(SUM(used_gt_6mo_y1) / SUM(amount_port_gt_6mo_y1) * 100, 2), 0), '%')`.as('occ_gt_6mo_y1'),
                    occ_all_mo_y1: sql<string>`CONCAT(COALESCE(ROUND(SUM(used_all_mo_y1) / SUM(amount_port_all_mo_y1) * 100, 2), 0), '%')`.as('occ_all_mo_y1'),
                })
                .from(db
                    .select({
                        regional: sql<string>`CASE WHEN telkomsel_regional IN ('PUMA', 'MALUKU DAN PAPUA') THEN 'MALUKU DAN PAPUA' END`.as('telkomsel_regional'),

                        avai_port_1mo_y: sql<number>`SUM(CASE WHEN DATEDIFF((SELECT MAX(tanggal_go_live_uim) FROM household.ih_occ_golive_ihld), tanggal_go_live_uim) <= 30 AND YEAR(tanggal_go_live_uim) = ${currYear} THEN avai END)`.as('avai_port_1mo_y'),
                        avai_port_2mo_y: sql<number>`SUM(CASE WHEN DATEDIFF((SELECT MAX(tanggal_go_live_uim) FROM household.ih_occ_golive_ihld), tanggal_go_live_uim) <= 60 AND YEAR(tanggal_go_live_uim) = ${currYear} THEN avai END)`.as('avai_port_2mo_y'),
                        avai_port_3mo_y: sql<number>`SUM(CASE WHEN DATEDIFF((SELECT MAX(tanggal_go_live_uim) FROM household.ih_occ_golive_ihld), tanggal_go_live_uim) <= 90 AND YEAR(tanggal_go_live_uim) = ${currYear} THEN avai END)`.as('avai_port_3mo_y'),
                        avai_port_4mo_y: sql<number>`SUM(CASE WHEN DATEDIFF((SELECT MAX(tanggal_go_live_uim) FROM household.ih_occ_golive_ihld), tanggal_go_live_uim) <= 180 AND YEAR(tanggal_go_live_uim) = ${currYear} THEN avai END)`.as('avai_port_4mo_y'),

                        used_1mo_y: sql<number>`SUM(CASE WHEN DATEDIFF((SELECT MAX(tanggal_go_live_uim) FROM household.ih_occ_golive_ihld), tanggal_go_live_uim) <= 30 AND YEAR(tanggal_go_live_uim) = ${currYear} THEN used END)`.as('used_1mo_y'),
                        used_2mo_y: sql<number>`SUM(CASE WHEN DATEDIFF((SELECT MAX(tanggal_go_live_uim) FROM household.ih_occ_golive_ihld), tanggal_go_live_uim) <= 60 AND YEAR(tanggal_go_live_uim) = ${currYear} THEN used END)`.as('used_2mo_y'),
                        used_3mo_y: sql<number>`SUM(CASE WHEN DATEDIFF((SELECT MAX(tanggal_go_live_uim) FROM household.ih_occ_golive_ihld), tanggal_go_live_uim) <= 90 AND YEAR(tanggal_go_live_uim) = ${currYear} THEN used END)`.as('used_3mo_y'),
                        used_4mo_y: sql<number>`SUM(CASE WHEN DATEDIFF((SELECT MAX(tanggal_go_live_uim) FROM household.ih_occ_golive_ihld), tanggal_go_live_uim) <= 180 AND YEAR(tanggal_go_live_uim) = ${currYear} THEN used END)`.as('used_4mo_y'),
                        used_gt_6mo_y: sql<number>`SUM(CASE WHEN DATEDIFF((SELECT MAX(tanggal_go_live_uim) FROM household.ih_occ_golive_ihld), tanggal_go_live_uim) > 180 AND YEAR(tanggal_go_live_uim) = ${currYear} THEN used END)`.as('used_gt_6mo_y'),
                        used_all_mo_y: sql<number>`SUM(CASE WHEN YEAR(tanggal_go_live_uim) = ${currYear} THEN used END)`.as('used_all_mo_y'),

                        used_1mo_y1: sql<number>`SUM(CASE WHEN DATEDIFF((SELECT MAX(tanggal_go_live_uim) FROM household.ih_occ_golive_ihld), tanggal_go_live_uim) <= 30 AND YEAR(tanggal_go_live_uim) = ${prevYear} THEN used END)`.as('used_1mo_y1'),
                        used_2mo_y1: sql<number>`SUM(CASE WHEN DATEDIFF((SELECT MAX(tanggal_go_live_uim) FROM household.ih_occ_golive_ihld), tanggal_go_live_uim) <= 60 AND YEAR(tanggal_go_live_uim) = ${prevYear} THEN used END)`.as('used_2mo_y1'),
                        used_3mo_y1: sql<number>`SUM(CASE WHEN DATEDIFF((SELECT MAX(tanggal_go_live_uim) FROM household.ih_occ_golive_ihld), tanggal_go_live_uim) <= 90 AND YEAR(tanggal_go_live_uim) = ${prevYear} THEN used END)`.as('used_3mo_y1'),
                        used_4mo_y1: sql<number>`SUM(CASE WHEN DATEDIFF((SELECT MAX(tanggal_go_live_uim) FROM household.ih_occ_golive_ihld), tanggal_go_live_uim) <= 180 AND YEAR(tanggal_go_live_uim) = ${prevYear} THEN used END)`.as('used_4mo_y1'),
                        used_gt_6mo_y1: sql<number>`SUM(CASE WHEN DATEDIFF((SELECT MAX(tanggal_go_live_uim) FROM household.ih_occ_golive_ihld), tanggal_go_live_uim) > 180 AND YEAR(tanggal_go_live_uim) = ${prevYear} THEN used END)`.as('used_gt_6mo_y1'),
                        used_all_mo_y1: sql<number>`SUM(CASE WHEN YEAR(tanggal_go_live_uim) = ${prevYear} THEN used END)`.as('used_all_mo_y1'),

                        amount_port_1mo_y: sql<number>`SUM(CASE WHEN DATEDIFF((SELECT MAX(tanggal_go_live_uim) FROM household.ih_occ_golive_ihld), tanggal_go_live_uim) <= 30 AND YEAR(tanggal_go_live_uim) = ${currYear} THEN is_total END)`.as('amount_port_1mo_y'),
                        amount_port_2mo_y: sql<number>`SUM(CASE WHEN DATEDIFF((SELECT MAX(tanggal_go_live_uim) FROM household.ih_occ_golive_ihld), tanggal_go_live_uim) <= 60 AND YEAR(tanggal_go_live_uim) = ${currYear} THEN is_total END)`.as('amount_port_2mo_y'),
                        amount_port_3mo_y: sql<number>`SUM(CASE WHEN DATEDIFF((SELECT MAX(tanggal_go_live_uim) FROM household.ih_occ_golive_ihld), tanggal_go_live_uim) <= 90 AND YEAR(tanggal_go_live_uim) = ${currYear} THEN is_total END)`.as('amount_port_3mo_y'),
                        amount_port_4mo_y: sql<number>`SUM(CASE WHEN DATEDIFF((SELECT MAX(tanggal_go_live_uim) FROM household.ih_occ_golive_ihld), tanggal_go_live_uim) <= 180 AND YEAR(tanggal_go_live_uim) = ${currYear} THEN is_total END)`.as('amount_port_4mo_y'),
                        amount_port_gt_6mo_y: sql<number>`SUM(CASE WHEN DATEDIFF((SELECT MAX(tanggal_go_live_uim) FROM household.ih_occ_golive_ihld), tanggal_go_live_uim) > 180 AND YEAR(tanggal_go_live_uim) = ${currYear} THEN is_total END)`.as('amount_port_gt_6mo_y'),
                        amount_port_all_mo_y: sql<number>`SUM(CASE WHEN YEAR(tanggal_go_live_uim) = ${currYear} THEN is_total END)`.as('amount_port_all_mo_y'),

                        amount_port_1mo_y1: sql<number>`SUM(CASE WHEN DATEDIFF((SELECT MAX(tanggal_go_live_uim) FROM household.ih_occ_golive_ihld), tanggal_go_live_uim) <= 30 AND YEAR(tanggal_go_live_uim) = ${prevYear} THEN is_total END)`.as('amount_port_1mo_y1'),
                        amount_port_2mo_y1: sql<number>`SUM(CASE WHEN DATEDIFF((SELECT MAX(tanggal_go_live_uim) FROM household.ih_occ_golive_ihld), tanggal_go_live_uim) <= 60 AND YEAR(tanggal_go_live_uim) = ${prevYear} THEN is_total END)`.as('amount_port_2mo_y1'),
                        amount_port_3mo_y1: sql<number>`SUM(CASE WHEN DATEDIFF((SELECT MAX(tanggal_go_live_uim) FROM household.ih_occ_golive_ihld), tanggal_go_live_uim) <= 90 AND YEAR(tanggal_go_live_uim) = ${prevYear} THEN is_total END)`.as('amount_port_3mo_y1'),
                        amount_port_4mo_y1: sql<number>`SUM(CASE WHEN DATEDIFF((SELECT MAX(tanggal_go_live_uim) FROM household.ih_occ_golive_ihld), tanggal_go_live_uim) <= 180 AND YEAR(tanggal_go_live_uim) = ${prevYear} THEN is_total END)`.as('amount_port_4mo_y1'),
                        amount_port_gt_6mo_y1: sql<number>`SUM(CASE WHEN DATEDIFF((SELECT MAX(tanggal_go_live_uim) FROM household.ih_occ_golive_ihld), tanggal_go_live_uim) > 180 AND YEAR(tanggal_go_live_uim) = ${prevYear} THEN is_total END)`.as('amount_port_gt_6mo_y1'),
                        amount_port_all_mo_y1: sql<number>`SUM(CASE WHEN YEAR(tanggal_go_live_uim) = ${prevYear} THEN is_total END)`.as('amount_port_all_mo_y1'),
                    })
                    .from(ih_occ_golive_ihld)
                    .where(eq(ih_occ_golive_ihld.telkomsel_regional, 'MALUKU DAN PAPUA'))
                    .groupBy(sql`1`)
                    .as('ff')
                )
                .groupBy(sql`1`)
                .as('f')

            const regionalTargetDemands = db
                .select({
                    regional: sql<string>`b.regional`.as('regional'),
                    ytd_demands: sum(targetTerritoryDemands.ytd_demand).as('ytd_demands')
                })
                .from(targetTerritoryDemands)
                .rightJoin(
                    db
                        .selectDistinct({ regional: territoryHousehold.regional, branch: territoryHousehold.branch, wok: territoryHousehold.wok })
                        .from(territoryHousehold)
                        .as('b'),
                    and(
                        eq(targetTerritoryDemands.branch, sql`b.branch`),
                        eq(targetTerritoryDemands.wok, sql`b.wok`)
                    )
                )
                .where(eq(targetTerritoryDemands.periode, currYear))
                .groupBy(sql`1`)
                .as('t')

            const regionalFinalQuery = db
                .select({
                    name: regionalTerritory.regional,
                    level: sql<string>`'regional'`.as('level'),

                    used_black: sql<number>`COALESCE(b.used_black, 0)`.as('used_black'),
                    used_red: sql<number>`b.used_red`.as('used_red'),
                    used_yellow: sql<number>`b.used_yellow`.as('used_yellow'),
                    used_green: sql<number>`b.used_green`.as('used_green'),
                    used: sql<number>`b.used`.as('used'),

                    used_black_m1: sql<number>`COALESCE(c.used_black, 0)`.as('used_black_m1'),
                    used_red_m1: sql<number>`c.used_red`.as('used_red_m1'),
                    used_yellow_m1: sql<number>`c.used_yellow`.as('used_yellow_m1'),
                    used_green_m1: sql<number>`c.used_green`.as('used_green_m1'),
                    used_m1: sql<number>`c.used`.as('used_m1'),

                    avai_port_black: sql<number>`COALESCE(b.avai_port_black, 0)`.as('avai_port_black'),
                    avai_port_red: sql<number>`b.avai_port_red`.as('avai_port_red'),
                    avai_port_yellow: sql<number>`b.avai_port_yellow`.as('avai_port_yellow'),
                    avai_port_green: sql<number>`b.avai_port_green`.as('avai_port_green'),
                    avai_port: sql<number>`b.avai_port`.as('avai_port'),

                    amount_port_black: sql<number>`COALESCE(b.amount_port_black, 0)`.as('amount_port_black'),
                    amount_port_red: sql<number>`b.amount_port_red`.as('amount_port_red'),
                    amount_port_yellow: sql<number>`b.amount_port_yellow`.as('amount_port_yellow'),
                    amount_port_green: sql<number>`b.amount_port_green`.as('amount_port_green'),
                    amount_port: sql<number>`b.amount_port`.as('amount_port'),

                    amount_port_black_m1: sql<number>`COALESCE(c.amount_port_black, 0)`.as('amount_port_black_m1'),
                    amount_port_red_m1: sql<number>`c.amount_port_red`.as('amount_port_red_m1'),
                    amount_port_yellow_m1: sql<number>`c.amount_port_yellow`.as('amount_port_yellow_m1'),
                    amount_port_green_m1: sql<number>`c.amount_port_green`.as('amount_port_green_m1'),
                    amount_port_m1: sql<number>`c.amount_port`.as('amount_port_m1'),

                    total_odp_black: sql<number>`COALESCE(b.total_odp_black, 0)`.as('total_odp_black'),
                    total_odp_red: sql<number>`b.total_odp_red`.as('total_odp_red'),
                    total_odp_yellow: sql<number>`b.total_odp_yellow`.as('total_odp_yellow'),
                    total_odp_green: sql<number>`b.total_odp_green`.as('total_odp_green'),
                    total_odp: sql<number>`b.total_odp`.as('total_odp'),

                    golive_m: sql<number>`d.golive_m`.as('golive_m'),
                    golive_m1: sql<number>`d.golive_m1`.as('golive_m1'),
                    golive_mom: sql<string>`d.golive_mom`.as('golive_mom'),
                    golive_y: sql<number>`d.golive_y`.as('golive_y'),
                    golive_y1: sql<number>`d.golive_y1`.as('golive_y1'),
                    golive_yoy: sql<string>`d.golive_yoy`.as('golive_yoy'),
                    golive_ytd: sql<number>`d.golive_ytd`.as('golive_ytd'),
                    golive_ytd1: sql<number>`d.golive_ytd1`.as('golive_ytd1'),
                    golive_ytd_per: sql<string>`d.golive_ytd_per`.as('golive_ytd_per'),

                    target_ytd_demand: sql<number>`t.ytd_demands`.as('target_ytd_demand'),
                    demand_created_mtd: sql<number>`COALESCE(e.demand_created_mtd, 0)`.as('demand_created_mtd'),
                    demand_created_m1: sql<number>`COALESCE(e.demand_created_m1, 0)`.as('demand_created_m1'),
                    demand_created_mom: sql<string>`CONCAT(COALESCE(ROUND((e.demand_created_mtd - e.demand_created_m1) / e.demand_created_m1 * 100, 2), 0), '%')`.as('demand_created_mom'),
                    ach_demands: sql<string>`CONCAT(COALESCE(ROUND(e.demand_created_mtd / t.ytd_demands * 100, 2) ,0), '%')`.as('ach_demands'),

                    avai_port_1mo_y: sql<number>`f.avai_port_1mo_y`.as('avai_port_1mo_y'),
                    avai_port_2mo_y: sql<number>`f.avai_port_2mo_y`.as('avai_port_2mo_y'),
                    avai_port_3mo_y: sql<number>`f.avai_port_3mo_y`.as('avai_port_3mo_y'),
                    avai_port_4mo_y: sql<number>`f.avai_port_4mo_y`.as('avai_port_4mo_y'),

                    used_1mo_y: sql<number>`f.used_1mo_y`.as('used_1mo_y'),
                    used_2mo_y: sql<number>`f.used_2mo_y`.as('used_2mo_y'),
                    used_3mo_y: sql<number>`f.used_3mo_y`.as('used_3mo_y'),
                    used_4mo_y: sql<number>`f.used_4mo_y`.as('used_4mo_y'),
                    used_gt_6mo_y: sql<number>`f.used_gt_6mo_y`.as('used_gt_6mo_y'),
                    used_all_mo_y: sql<number>`f.used_all_mo_y`.as('used_all_mo_y'),

                    amount_port_1mo_y: sql<number>`f.amount_port_1mo_y`.as('amount_port_1mo_y'),
                    amount_port_2mo_y: sql<number>`f.amount_port_2mo_y`.as('amount_port_2mo_y'),
                    amount_port_3mo_y: sql<number>`f.amount_port_3mo_y`.as('amount_port_3mo_y'),
                    amount_port_4mo_y: sql<number>`f.amount_port_4mo_y`.as('amount_port_4mo_y'),
                    amount_port_gt_6mo_y: sql<number>`f.amount_port_gt_6mo_y`.as('amount_port_gt_6mo_y'),
                    amount_port_all_mo_y: sql<number>`f.amount_port_all_mo_y`.as('amount_port_all_mo_y'),

                    used_1mo_y1: sql<number>`f.used_1mo_y1`.as('used_1mo_y1'),
                    used_2mo_y1: sql<number>`f.used_2mo_y1`.as('used_2mo_y1'),
                    used_3mo_y1: sql<number>`f.used_3mo_y1`.as('used_3mo_y1'),
                    used_4mo_y1: sql<number>`f.used_4mo_y1`.as('used_4mo_y1'),
                    used_gt_6mo_y1: sql<number>`f.used_gt_6mo_y1`.as('used_gt_6mo_y1'),
                    used_all_mo_y1: sql<number>`f.used_all_mo_y1`.as('used_all_mo_y1'),

                    amount_port_1mo_y1: sql<number>`f.amount_port_1mo_y1`.as('amount_port_1mo_y1'),
                    amount_port_2mo_y1: sql<number>`f.amount_port_2mo_y1`.as('amount_port_2mo_y1'),
                    amount_port_3mo_y1: sql<number>`f.amount_port_3mo_y1`.as('amount_port_3mo_y1'),
                    amount_port_4mo_y1: sql<number>`f.amount_port_4mo_y1`.as('amount_port_4mo_y1'),
                    amount_port_gt_6mo_y1: sql<number>`f.amount_port_gt_6mo_y1`.as('amount_port_gt_6mo_y1'),
                    amount_port_all_mo_y1: sql<number>`f.amount_port_all_mo_y1`.as('amount_port_all_mo_y1'),

                    occ_1mo_y: sql<string>`f.occ_1mo_y`.as('occ_1mo_y'),
                    occ_2mo_y: sql<string>`f.occ_2mo_y`.as('occ_2mo_y'),
                    occ_3mo_y: sql<string>`f.occ_3mo_y`.as('occ_3mo_y'),
                    occ_4mo_y: sql<string>`f.occ_4mo_y`.as('occ_4mo_y'),
                    occ_gt_6mo_y: sql<string>`f.occ_gt_6mo_y`.as('occ_gt_6mo_y'),
                    occ_all_mo_y: sql<string>`f.occ_all_mo_y`.as('occ_all_mo_y'),

                    occ_1mo_y1: sql<string>`f.occ_1mo_y1`.as('occ_1mo_y1'),
                    occ_2mo_y1: sql<string>`f.occ_2mo_y1`.as('occ_2mo_y1'),
                    occ_3mo_y1: sql<string>`f.occ_3mo_y1`.as('occ_3mo_y1'),
                    occ_4mo_y1: sql<string>`f.occ_4mo_y1`.as('occ_4mo_y1'),
                    occ_gt_6mo_y1: sql<string>`f.occ_gt_6mo_y1`.as('occ_gt_6mo_y1'),
                    occ_all_mo_y1: sql<string>`f.occ_all_mo_y1`.as('occ_all_mo_y1'),

                    occ_1mo_2y: sql<string>`CONCAT(ROUND((f.used_1mo_y + f.used_1mo_y1) / (f.amount_port_1mo_y + f.amount_port_1mo_y1) * 100 ,2), '%')`.as('occ_1mo_2y'),
                    occ_2mo_2y: sql<string>`CONCAT(ROUND((f.used_2mo_y + f.used_2mo_y1) / (f.amount_port_2mo_y + f.amount_port_2mo_y1) * 100 ,2), '%')`.as('occ_2mo_2y'),
                    occ_3mo_2y: sql<string>`CONCAT(ROUND((f.used_3mo_y + f.used_3mo_y1) / (f.amount_port_3mo_y + f.amount_port_3mo_y1) * 100 ,2), '%')`.as('occ_3mo_2y'),
                    occ_4mo_2y: sql<string>`CONCAT(ROUND((f.used_4mo_y + f.used_4mo_y1) / (f.amount_port_4mo_y + f.amount_port_4mo_y1) * 100 ,2), '%')`.as('occ_4mo_2y'),
                    occ_gt_6mo_2y: sql<string>`CONCAT(ROUND((f.used_gt_6mo_y + f.used_gt_6mo_y1) / (f.amount_port_gt_6mo_y + f.amount_port_gt_6mo_y1) * 100 ,2), '%')`.as('occ_gt_6mo_2y'),
                    occ_all_mo_2y: sql<string>`CONCAT(ROUND((f.used_all_mo_y + f.used_all_mo_y1) / (f.amount_port_all_mo_y + f.amount_port_all_mo_y1) * 100 ,2), '%')`.as('occ_all_mo_2y'),
                })
                .from(regionalTerritory)
                .leftJoin(regionalCurrOdp, sql`${regionalTerritory.regional} = b.regional`)
                .leftJoin(regionalPrevOdp, sql`${regionalTerritory.regional} = c.regional`)
                .leftJoin(regionalGoLive, sql`${regionalTerritory.regional} = d.regional`)
                .leftJoin(regionalDemands, eq(regionalTerritory.regional, regionalDemands.regional))
                .leftJoin(regionalGoLiveCurrYear, sql`${regionalTerritory.regional} = f.regional`)
                .leftJoin(regionalTargetDemands, sql`${regionalTerritory.regional} = t.regional`)
                .groupBy(sql`1,2`)

            const branchTerritory = db
                .select({ branch: territoryHousehold.branch })
                .from(territoryHousehold)
                .where(branch ? and(
                    eq(territoryHousehold.regional, 'MALUKU DAN PAPUA'),
                    eq(territoryHousehold.branch, branch)
                ) : eq(territoryHousehold.regional, 'MALUKU DAN PAPUA'))
                .groupBy(territoryHousehold.branch)
                .as('a');

            const branchCurrOdp = db
                .select({
                    branch: sql<string>`bb.branch`.as('branch'),
                    used_black: sql<number>`SUM(CASE WHEN status = 'BLACK' THEN used END)`.as('used_black'),
                    used_red: sql<number>`SUM(CASE WHEN status = 'RED' THEN used END)`.as('used_red'),
                    used_yellow: sql<number>`SUM(CASE WHEN status = 'YELLOW' THEN used END)`.as('used_yellow'),
                    used_green: sql<number>`SUM(CASE WHEN status = 'GREEN' THEN used END)`.as('used_green'),
                    used: sql<number>`SUM(used)`.as('used'),

                    avai_port_black: sql<number>`SUM(CASE WHEN status = 'BLACK' THEN avai_port END)`.as('avai_port_black'),
                    avai_port_red: sql<number>`SUM(CASE WHEN status = 'RED' THEN avai_port END)`.as('avai_port_red'),
                    avai_port_yellow: sql<number>`SUM(CASE WHEN status = 'YELLOW' THEN avai_port END)`.as('avai_port_yellow'),
                    avai_port_green: sql<number>`SUM(CASE WHEN status = 'GREEN' THEN avai_port END)`.as('avai_port_green'),
                    avai_port: sql<number>`SUM(avai_port)`.as('avai_port'),

                    amount_port_black: sql<number>`SUM(CASE WHEN status = 'BLACK' THEN amount_port END)`.as('amount_port_black'),
                    amount_port_red: sql<number>`SUM(CASE WHEN status = 'RED' THEN amount_port END)`.as('amount_port_red'),
                    amount_port_yellow: sql<number>`SUM(CASE WHEN status = 'YELLOW' THEN amount_port END)`.as('amount_port_yellow'),
                    amount_port_green: sql<number>`SUM(CASE WHEN status = 'GREEN' THEN amount_port END)`.as('amount_port_green'),
                    amount_port: sql<number>`SUM(amount_port)`.as('amount_port'),

                    total_odp_black: sql<number>`SUM(CASE WHEN status = 'BLACK' THEN odp END)`.as('total_odp_black'),
                    total_odp_red: sql<number>`SUM(CASE WHEN status = 'RED' THEN odp END)`.as('total_odp_red'),
                    total_odp_yellow: sql<number>`SUM(CASE WHEN status = 'YELLOW' THEN odp END)`.as('total_odp_yellow'),
                    total_odp_green: sql<number>`SUM(CASE WHEN status = 'GREEN' THEN odp END)`.as('total_odp_green'),
                    total_odp: sql<number>`SUM(bb.odp)`.as('total_odp')
                })
                .from(db
                    .select({
                        branch: sql<string>`
                    CASE
                        WHEN sto IN ('WHA','TUA','SPR','SML','NML','NIR','MSH','LRA','DOB','BUL', 'PAO', 'ABO') THEN 'AMBON'
                        WHEN sto IN ('WAM','SRU','SRM','SNI','BIA', 'WAE','JPB','JAP','ABE') THEN 'JAYAPURA'
                        WHEN sto IN ('TIM','TBG','KUK','NAB', 'TMR','MRK','BAD','AGT') THEN 'TIMIKA'
                        WHEN sto IN ('WMR','RSK','MWR','KIN','FFA','BTI', 'TMB', 'SON') THEN 'SORONG'
                    END`.as('branch'),
                        status: sql<string>`
                    CASE
                        WHEN ${currOdpTable.status} IN ('BLACK', 'BLACKSYSTEM') THEN 'BLACK' ELSE ${currOdpTable.status}
                    END`.as('status'),
                        amount_port: sum(currOdpTable.is_total).as('amount_port'),
                        avai_port: sum(currOdpTable.avai).as('avai_port'),
                        used: sum(currOdpTable.used).as('used'),
                        odp: count(currOdpTable.status).as('odp')
                    })
                    .from(currOdpTable)
                    .where(inArray(currOdpTable.witel, ['MALUKU', 'PAPUA', 'PAPUA BARAT']))
                    .groupBy(sql`1,2`)
                    .as('bb')
                )
                .groupBy(sql`1`)
                .as('b')

            const branchPrevOdp = db
                .select({
                    branch: sql<string>`cc.branch`.as('branch'),
                    used_black: sql<number>`SUM(CASE WHEN status = 'BLACK' THEN used END)`.as('used_black'),
                    used_red: sql<number>`SUM(CASE WHEN status = 'RED' THEN used END)`.as('used_red'),
                    used_yellow: sql<number>`SUM(CASE WHEN status = 'YELLOW' THEN used END)`.as('used_yellow'),
                    used_green: sql<number>`SUM(CASE WHEN status = 'GREEN' THEN used END)`.as('used_green'),
                    used: sql<number>`SUM(used)`.as('used'),

                    amount_port_black: sql<number>`SUM(CASE WHEN status = 'BLACK' THEN amount_port END)`.as('amount_port_black'),
                    amount_port_red: sql<number>`SUM(CASE WHEN status = 'RED' THEN amount_port END)`.as('amount_port_red'),
                    amount_port_yellow: sql<number>`SUM(CASE WHEN status = 'YELLOW' THEN amount_port END)`.as('amount_port_yellow'),
                    amount_port_green: sql<number>`SUM(CASE WHEN status = 'GREEN' THEN amount_port END)`.as('amount_port_green'),
                    amount_port: sql<number>`SUM(amount_port)`.as('amount_port'),
                })
                .from(db
                    .select({
                        branch: sql<string>`
                    CASE
                        WHEN ${prevOdpTable.sto} IN ('WHA','TUA','SPR','SML','NML','NIR','MSH','LRA','DOB','BUL', 'PAO', 'ABO') THEN 'AMBON'
                        WHEN ${prevOdpTable.sto} IN ('WAM','SRU','SRM','SNI','BIA', 'WAE','JPB','JAP','ABE') THEN 'JAYAPURA'
                        WHEN ${prevOdpTable.sto} IN ('TIM','TBG','KUK','NAB', 'TMR','MRK','BAD','AGT') THEN 'TIMIKA'
                        WHEN ${prevOdpTable.sto} IN ('WMR','RSK','MWR','KIN','FFA','BTI', 'TMB', 'SON') THEN 'SORONG'
                    END`.as('branch'),
                        status: sql<string>`
                    CASE
                        WHEN ${prevOdpTable.status} IN ('BLACK', 'BLACKSYSTEM') THEN 'BLACK' ELSE ${prevOdpTable.status}
                    END`.as('status'),
                        used: sum(prevOdpTable.used).as('used'),
                        amount_port: sum(prevOdpTable.is_total).as('amount_port')
                    })
                    .from(prevOdpTable)
                    .where(inArray(prevOdpTable.witel, ['MALUKU', 'PAPUA', 'PAPUA BARAT']))
                    .groupBy(sql`1,2`)
                    .as('cc')
                )
                .groupBy(sql`1`)
                .as('c');

            const branchGoLive = db
                .select({
                    branch: sql<string>`${ih_occ_golive_ihld.telkomsel_branch}`.as('branch'),
                    golive_m: sql<number>`COUNT(CASE WHEN ${ih_occ_golive_ihld.tanggal_go_live_uim} BETWEEN ${currStartOfMonth} AND ${currDate} THEN 1 END)`.as('golive_m'),
                    golive_m1: sql<number>`COUNT(CASE WHEN ${ih_occ_golive_ihld.tanggal_go_live_uim} BETWEEN ${prevStartOfMonth} AND ${prevDate2} THEN 1 END)`.as('golive_m1'),

                    // Add y and y1 calculations here directly
                    golive_y: sql<number>`COUNT(CASE WHEN tahun_go_live_uim = 2025 THEN 1 END)`.as('golive_y'),
                    golive_y1: sql<number>`COUNT(CASE WHEN tahun_go_live_uim = 2024 THEN 1 END)`.as('golive_y1'),

                    golive_ytd: sql<number>`COUNT(CASE WHEN ${ih_occ_golive_ihld.tanggal_go_live_uim} BETWEEN ${currJanuaryFirst} AND ${currDate} THEN 1 END)`.as('golive_ytd'),
                    golive_ytd1: sql<number>`COUNT(CASE WHEN ${ih_occ_golive_ihld.tanggal_go_live_uim} BETWEEN ${prevJanuaryFirst} AND ${prevYearCurrDate} THEN 1 END)`.as('golive_ytd1'),

                    // Calculate MoM and YoY percentages here as well
                    golive_mom: sql<string>`CONCAT(COALESCE(ROUND((COUNT(CASE WHEN ${ih_occ_golive_ihld.tanggal_go_live_uim} BETWEEN ${currStartOfMonth} AND ${currDate} THEN 1 END) - COUNT(CASE WHEN ${ih_occ_golive_ihld.tanggal_go_live_uim} BETWEEN ${prevStartOfMonth} AND ${prevDate2} THEN 1 END)) / COUNT(CASE WHEN ${ih_occ_golive_ihld.tanggal_go_live_uim} BETWEEN ${prevStartOfMonth} AND ${prevDate2} THEN 1 END) * 100, 2),0), '%')`.as('golive_mom'),
                    golive_yoy: sql<string>`CONCAT(COALESCE(ROUND((COUNT(CASE WHEN tahun_go_live_uim = 2025 THEN 1 END) - COUNT(CASE WHEN tahun_go_live_uim = 2024 THEN 1 END)) / COUNT(CASE WHEN tahun_go_live_uim = 2024 THEN 1 END) * 100, 2),0), '%')`.as('golive_yoy'),
                    golive_ytd_per: sql<string>`CONCAT(COALESCE(ROUND((COUNT(CASE WHEN ${ih_occ_golive_ihld.tanggal_go_live_uim} BETWEEN ${currJanuaryFirst} AND ${currDate} THEN 1 END) - COUNT(CASE WHEN ${ih_occ_golive_ihld.tanggal_go_live_uim} BETWEEN ${prevJanuaryFirst} AND ${prevYearCurrDate} THEN 1 END)) / COUNT(CASE WHEN ${ih_occ_golive_ihld.tanggal_go_live_uim} BETWEEN ${prevJanuaryFirst} AND ${prevYearCurrDate} THEN 1 END) * 100, 2),0), '%')`.as('golive_ytd_per'),
                })
                .from(ih_occ_golive_ihld)
                .groupBy(sql`1`)
                .as('d')

            const branchDemands = db
                .select({
                    branch: ih_demand_mysiis.branch,
                    demand_created_mtd: sql<number>`SUM(CASE WHEN DATE_FORMAT(approved_at, '%Y-%m-%d') BETWEEN ${currStartOfMonth} AND ${currDate} THEN ${ih_demand_mysiis.target} END)`.as('demand_created_mtd'),
                    demand_created_m1: sql<number>`SUM(CASE WHEN DATE_FORMAT(approved_at, '%Y-%m-%d') BETWEEN ${prevStartOfMonth} AND ${prevMonthSameDate} THEN ${ih_demand_mysiis.target} END)`.as('demand_created_m1')
                })
                .from(ih_demand_mysiis)
                .where(and(
                    gt(ih_demand_mysiis.target, 0),
                    eq(ih_demand_mysiis.region, 'MALUKU DAN PAPUA'),
                    inArray(ih_demand_mysiis.status_new_bispro, ['Wait Approval AI', 'Draft']),
                    sql`YEAR(${ih_demand_mysiis.approved_at}) = ${currYear}`
                ))
                .groupBy(sql`1`)
                .as('e')

            const branchGoLiveCurrYear = db
                .select({
                    branch: sql<string>`ff.telkomsel_branch`.as('branch'),

                    avai_port_1mo_y: sql<number>`COALESCE(ff.avai_port_1mo_y, 0)`.as('avai_port_1mo_y'),
                    avai_port_2mo_y: sql<number>`COALESCE(ff.avai_port_2mo_y, 0)`.as('avai_port_2mo_y'),
                    avai_port_3mo_y: sql<number>`COALESCE(ff.avai_port_3mo_y, 0)`.as('avai_port_3mo_y'),
                    avai_port_4mo_y: sql<number>`COALESCE(ff.avai_port_4mo_y, 0)`.as('avai_port_4mo_y'),

                    used_1mo_y: sql<number>`COALESCE(ff.used_1mo_y, 0)`.as('used_1mo_y'),
                    used_2mo_y: sql<number>`COALESCE(ff.used_2mo_y, 0)`.as('used_2mo_y'),
                    used_3mo_y: sql<number>`COALESCE(ff.used_3mo_y, 0)`.as('used_3mo_y'),
                    used_4mo_y: sql<number>`COALESCE(ff.used_4mo_y, 0)`.as('used_4mo_y'),
                    used_gt_6mo_y: sql<number>`COALESCE(ff.used_gt_6mo_y, 0)`.as('used_gt_6mo_y'),
                    used_all_mo_y: sql<number>`COALESCE(ff.used_all_mo_y, 0)`.as('used_all_mo_y'),

                    amount_port_1mo_y: sql<number>`COALESCE(ff.amount_port_1mo_y, 0)`.as('amount_port_1mo_y'),
                    amount_port_2mo_y: sql<number>`COALESCE(ff.amount_port_2mo_y, 0)`.as('amount_port_2mo_y'),
                    amount_port_3mo_y: sql<number>`COALESCE(ff.amount_port_3mo_y, 0)`.as('amount_port_3mo_y'),
                    amount_port_4mo_y: sql<number>`COALESCE(ff.amount_port_4mo_y, 0)`.as('amount_port_4mo_y'),
                    amount_port_gt_6mo_y: sql<number>`COALESCE(ff.amount_port_gt_6mo_y, 0)`.as('amount_port_gt_6mo_y'),
                    amount_port_all_mo_y: sql<number>`COALESCE(ff.amount_port_all_mo_y, 0)`.as('amount_port_all_mo_y'),

                    used_1mo_y1: sql<number>`COALESCE(ff.used_1mo_y1, 0)`.as('used_1mo_y1'),
                    used_2mo_y1: sql<number>`COALESCE(ff.used_2mo_y1, 0)`.as('used_2mo_y1'),
                    used_3mo_y1: sql<number>`COALESCE(ff.used_3mo_y1, 0)`.as('used_3mo_y1'),
                    used_4mo_y1: sql<number>`COALESCE(ff.used_4mo_y1, 0)`.as('used_4mo_y1'),
                    used_gt_6mo_y1: sql<number>`COALESCE(ff.used_gt_6mo_y1, 0)`.as('used_gt_6mo_y1'),
                    used_all_mo_y1: sql<number>`COALESCE(ff.used_all_mo_y1, 0)`.as('used_all_mo_y1'),

                    amount_port_1mo_y1: sql<number>`COALESCE(ff.amount_port_1mo_y1, 0)`.as('amount_port_1mo_y1'),
                    amount_port_2mo_y1: sql<number>`COALESCE(ff.amount_port_2mo_y1, 0)`.as('amount_port_2mo_y1'),
                    amount_port_3mo_y1: sql<number>`COALESCE(ff.amount_port_3mo_y1, 0)`.as('amount_port_3mo_y1'),
                    amount_port_4mo_y1: sql<number>`COALESCE(ff.amount_port_4mo_y1, 0)`.as('amount_port_4mo_y1'),
                    amount_port_gt_6mo_y1: sql<number>`COALESCE(ff.amount_port_gt_6mo_y1, 0)`.as('amount_port_gt_6mo_y1'),
                    amount_port_all_mo_y1: sql<number>`COALESCE(ff.amount_port_all_mo_y1, 0)`.as('amount_port_all_mo_y1'),

                    occ_1mo_y: sql<string>`CONCAT(COALESCE(ROUND(SUM(used_1mo_y) / SUM(amount_port_1mo_y) * 100, 2), 0), '%')`.as('occ_1mo_y'),
                    occ_2mo_y: sql<string>`CONCAT(COALESCE(ROUND(SUM(used_2mo_y) / SUM(amount_port_2mo_y) * 100, 2), 0), '%')`.as('occ_2mo_y'),
                    occ_3mo_y: sql<string>`CONCAT(COALESCE(ROUND(SUM(used_3mo_y) / SUM(amount_port_3mo_y) * 100, 2), 0), '%')`.as('occ_3mo_y'),
                    occ_4mo_y: sql<string>`CONCAT(COALESCE(ROUND(SUM(used_4mo_y) / SUM(amount_port_4mo_y) * 100, 2), 0), '%')`.as('occ_4mo_y'),
                    occ_gt_6mo_y: sql<string>`CONCAT(COALESCE(ROUND(SUM(used_gt_6mo_y) / SUM(amount_port_gt_6mo_y) * 100, 2), 0), '%')`.as('occ_gt_6mo_y'),
                    occ_all_mo_y: sql<string>`CONCAT(COALESCE(ROUND(SUM(used_all_mo_y) / SUM(amount_port_all_mo_y) * 100, 2), 0), '%')`.as('occ_all_mo_y'),

                    occ_1mo_y1: sql<string>`CONCAT(COALESCE(ROUND(SUM(used_1mo_y1) / SUM(amount_port_1mo_y1) * 100, 2), 0), '%')`.as('occ_1mo_y1'),
                    occ_2mo_y1: sql<string>`CONCAT(COALESCE(ROUND(SUM(used_2mo_y1) / SUM(amount_port_2mo_y1) * 100, 2), 0), '%')`.as('occ_2mo_y1'),
                    occ_3mo_y1: sql<string>`CONCAT(COALESCE(ROUND(SUM(used_3mo_y1) / SUM(amount_port_3mo_y1) * 100, 2), 0), '%')`.as('occ_3mo_y1'),
                    occ_4mo_y1: sql<string>`CONCAT(COALESCE(ROUND(SUM(used_4mo_y1) / SUM(amount_port_4mo_y1) * 100, 2), 0), '%')`.as('occ_4mo_y1'),
                    occ_gt_6mo_y1: sql<string>`CONCAT(COALESCE(ROUND(SUM(used_gt_6mo_y1) / SUM(amount_port_gt_6mo_y1) * 100, 2), 0), '%')`.as('occ_gt_6mo_y1'),
                    occ_all_mo_y1: sql<string>`CONCAT(COALESCE(ROUND(SUM(used_all_mo_y1) / SUM(amount_port_all_mo_y1) * 100, 2), 0), '%')`.as('occ_all_mo_y1'),
                })
                .from(db
                    .select({
                        branch: ih_occ_golive_ihld.telkomsel_branch,

                        avai_port_1mo_y: sql<number>`SUM(CASE WHEN DATEDIFF((SELECT MAX(tanggal_go_live_uim) FROM household.ih_occ_golive_ihld), tanggal_go_live_uim) <= 30 AND YEAR(tanggal_go_live_uim) = ${currYear} THEN avai END)`.as('avai_port_1mo_y'),
                        avai_port_2mo_y: sql<number>`SUM(CASE WHEN DATEDIFF((SELECT MAX(tanggal_go_live_uim) FROM household.ih_occ_golive_ihld), tanggal_go_live_uim) <= 60 AND YEAR(tanggal_go_live_uim) = ${currYear} THEN avai END)`.as('avai_port_2mo_y'),
                        avai_port_3mo_y: sql<number>`SUM(CASE WHEN DATEDIFF((SELECT MAX(tanggal_go_live_uim) FROM household.ih_occ_golive_ihld), tanggal_go_live_uim) <= 90 AND YEAR(tanggal_go_live_uim) = ${currYear} THEN avai END)`.as('avai_port_3mo_y'),
                        avai_port_4mo_y: sql<number>`SUM(CASE WHEN DATEDIFF((SELECT MAX(tanggal_go_live_uim) FROM household.ih_occ_golive_ihld), tanggal_go_live_uim) <= 180 AND YEAR(tanggal_go_live_uim) = ${currYear} THEN avai END)`.as('avai_port_4mo_y'),

                        used_1mo_y: sql<number>`SUM(CASE WHEN DATEDIFF((SELECT MAX(tanggal_go_live_uim) FROM household.ih_occ_golive_ihld), tanggal_go_live_uim) <= 30 AND YEAR(tanggal_go_live_uim) = ${currYear} THEN used END)`.as('used_1mo_y'),
                        used_2mo_y: sql<number>`SUM(CASE WHEN DATEDIFF((SELECT MAX(tanggal_go_live_uim) FROM household.ih_occ_golive_ihld), tanggal_go_live_uim) <= 60 AND YEAR(tanggal_go_live_uim) = ${currYear} THEN used END)`.as('used_2mo_y'),
                        used_3mo_y: sql<number>`SUM(CASE WHEN DATEDIFF((SELECT MAX(tanggal_go_live_uim) FROM household.ih_occ_golive_ihld), tanggal_go_live_uim) <= 90 AND YEAR(tanggal_go_live_uim) = ${currYear} THEN used END)`.as('used_3mo_y'),
                        used_4mo_y: sql<number>`SUM(CASE WHEN DATEDIFF((SELECT MAX(tanggal_go_live_uim) FROM household.ih_occ_golive_ihld), tanggal_go_live_uim) <= 180 AND YEAR(tanggal_go_live_uim) = ${currYear} THEN used END)`.as('used_4mo_y'),
                        used_gt_6mo_y: sql<number>`SUM(CASE WHEN DATEDIFF((SELECT MAX(tanggal_go_live_uim) FROM household.ih_occ_golive_ihld), tanggal_go_live_uim) > 180 AND YEAR(tanggal_go_live_uim) = ${currYear} THEN used END)`.as('used_gt_6mo_y'),
                        used_all_mo_y: sql<number>`SUM(CASE WHEN YEAR(tanggal_go_live_uim) = ${currYear} THEN used END)`.as('used_all_mo_y'),

                        used_1mo_y1: sql<number>`SUM(CASE WHEN DATEDIFF((SELECT MAX(tanggal_go_live_uim) FROM household.ih_occ_golive_ihld), tanggal_go_live_uim) <= 30 AND YEAR(tanggal_go_live_uim) = ${prevYear} THEN used END)`.as('used_1mo_y1'),
                        used_2mo_y1: sql<number>`SUM(CASE WHEN DATEDIFF((SELECT MAX(tanggal_go_live_uim) FROM household.ih_occ_golive_ihld), tanggal_go_live_uim) <= 60 AND YEAR(tanggal_go_live_uim) = ${prevYear} THEN used END)`.as('used_2mo_y1'),
                        used_3mo_y1: sql<number>`SUM(CASE WHEN DATEDIFF((SELECT MAX(tanggal_go_live_uim) FROM household.ih_occ_golive_ihld), tanggal_go_live_uim) <= 90 AND YEAR(tanggal_go_live_uim) = ${prevYear} THEN used END)`.as('used_3mo_y1'),
                        used_4mo_y1: sql<number>`SUM(CASE WHEN DATEDIFF((SELECT MAX(tanggal_go_live_uim) FROM household.ih_occ_golive_ihld), tanggal_go_live_uim) <= 180 AND YEAR(tanggal_go_live_uim) = ${prevYear} THEN used END)`.as('used_4mo_y1'),
                        used_gt_6mo_y1: sql<number>`SUM(CASE WHEN DATEDIFF((SELECT MAX(tanggal_go_live_uim) FROM household.ih_occ_golive_ihld), tanggal_go_live_uim) > 180 AND YEAR(tanggal_go_live_uim) = ${prevYear} THEN used END)`.as('used_gt_6mo_y1'),
                        used_all_mo_y1: sql<number>`SUM(CASE WHEN YEAR(tanggal_go_live_uim) = ${prevYear} THEN used END)`.as('used_all_mo_y1'),

                        amount_port_1mo_y: sql<number>`SUM(CASE WHEN DATEDIFF((SELECT MAX(tanggal_go_live_uim) FROM household.ih_occ_golive_ihld), tanggal_go_live_uim) <= 30 AND YEAR(tanggal_go_live_uim) = ${currYear} THEN is_total END)`.as('amount_port_1mo_y'),
                        amount_port_2mo_y: sql<number>`SUM(CASE WHEN DATEDIFF((SELECT MAX(tanggal_go_live_uim) FROM household.ih_occ_golive_ihld), tanggal_go_live_uim) <= 60 AND YEAR(tanggal_go_live_uim) = ${currYear} THEN is_total END)`.as('amount_port_2mo_y'),
                        amount_port_3mo_y: sql<number>`SUM(CASE WHEN DATEDIFF((SELECT MAX(tanggal_go_live_uim) FROM household.ih_occ_golive_ihld), tanggal_go_live_uim) <= 90 AND YEAR(tanggal_go_live_uim) = ${currYear} THEN is_total END)`.as('amount_port_3mo_y'),
                        amount_port_4mo_y: sql<number>`SUM(CASE WHEN DATEDIFF((SELECT MAX(tanggal_go_live_uim) FROM household.ih_occ_golive_ihld), tanggal_go_live_uim) <= 180 AND YEAR(tanggal_go_live_uim) = ${currYear} THEN is_total END)`.as('amount_port_4mo_y'),
                        amount_port_gt_6mo_y: sql<number>`SUM(CASE WHEN DATEDIFF((SELECT MAX(tanggal_go_live_uim) FROM household.ih_occ_golive_ihld), tanggal_go_live_uim) > 180 AND YEAR(tanggal_go_live_uim) = ${currYear} THEN is_total END)`.as('amount_port_gt_6mo_y'),
                        amount_port_all_mo_y: sql<number>`SUM(CASE WHEN YEAR(tanggal_go_live_uim) = ${currYear} THEN is_total END)`.as('amount_port_all_mo_y'),

                        amount_port_1mo_y1: sql<number>`SUM(CASE WHEN DATEDIFF((SELECT MAX(tanggal_go_live_uim) FROM household.ih_occ_golive_ihld), tanggal_go_live_uim) <= 30 AND YEAR(tanggal_go_live_uim) = ${prevYear} THEN is_total END)`.as('amount_port_1mo_y1'),
                        amount_port_2mo_y1: sql<number>`SUM(CASE WHEN DATEDIFF((SELECT MAX(tanggal_go_live_uim) FROM household.ih_occ_golive_ihld), tanggal_go_live_uim) <= 60 AND YEAR(tanggal_go_live_uim) = ${prevYear} THEN is_total END)`.as('amount_port_2mo_y1'),
                        amount_port_3mo_y1: sql<number>`SUM(CASE WHEN DATEDIFF((SELECT MAX(tanggal_go_live_uim) FROM household.ih_occ_golive_ihld), tanggal_go_live_uim) <= 90 AND YEAR(tanggal_go_live_uim) = ${prevYear} THEN is_total END)`.as('amount_port_3mo_y1'),
                        amount_port_4mo_y1: sql<number>`SUM(CASE WHEN DATEDIFF((SELECT MAX(tanggal_go_live_uim) FROM household.ih_occ_golive_ihld), tanggal_go_live_uim) <= 180 AND YEAR(tanggal_go_live_uim) = ${prevYear} THEN is_total END)`.as('amount_port_4mo_y1'),
                        amount_port_gt_6mo_y1: sql<number>`SUM(CASE WHEN DATEDIFF((SELECT MAX(tanggal_go_live_uim) FROM household.ih_occ_golive_ihld), tanggal_go_live_uim) > 180 AND YEAR(tanggal_go_live_uim) = ${prevYear} THEN is_total END)`.as('amount_port_gt_6mo_y1'),
                        amount_port_all_mo_y1: sql<number>`SUM(CASE WHEN YEAR(tanggal_go_live_uim) = ${prevYear} THEN is_total END)`.as('amount_port_all_mo_y1'),
                    })
                    .from(ih_occ_golive_ihld)
                    .where(eq(ih_occ_golive_ihld.telkomsel_regional, 'MALUKU DAN PAPUA'))
                    .groupBy(sql`1`)
                    .as('ff')
                )
                .groupBy(sql`1`)
                .as('f')

            const branchTargetDemands = db
                .select({
                    branch: targetTerritoryDemands.branch,
                    ytd_demands: sum(targetTerritoryDemands.ytd_demand).as('ytd_demands')
                })
                .from(targetTerritoryDemands)
                .rightJoin(
                    db
                        .selectDistinct({ branch: territoryHousehold.branch, wok: territoryHousehold.wok })
                        .from(territoryHousehold)
                        .as('b'),
                    and(
                        eq(targetTerritoryDemands.branch, sql`b.branch`),
                        eq(targetTerritoryDemands.wok, sql`b.wok`)
                    )
                )
                .groupBy(sql`1`)
                .as('t')

            const branchFinalQuery = db
                .select({
                    name: branchTerritory.branch,
                    level: sql<string>`'branch'`.as('level'),

                    used_black: sql<number>`COALESCE(b.used_black, 0)`.as('used_black'),
                    used_red: sql<number>`b.used_red`.as('used_red'),
                    used_yellow: sql<number>`b.used_yellow`.as('used_yellow'),
                    used_green: sql<number>`b.used_green`.as('used_green'),
                    used: sql<number>`b.used`.as('used'),

                    used_black_m1: sql<number>`COALESCE(c.used_black, 0)`.as('used_black_m1'),
                    used_red_m1: sql<number>`c.used_red`.as('used_red_m1'),
                    used_yellow_m1: sql<number>`c.used_yellow`.as('used_yellow_m1'),
                    used_green_m1: sql<number>`c.used_green`.as('used_green_m1'),
                    used_m1: sql<number>`c.used`.as('used_m1'),

                    avai_port_black: sql<number>`COALESCE(b.avai_port_black, 0)`.as('avai_port_black'),
                    avai_port_red: sql<number>`b.avai_port_red`.as('avai_port_red'),
                    avai_port_yellow: sql<number>`b.avai_port_yellow`.as('avai_port_yellow'),
                    avai_port_green: sql<number>`b.avai_port_green`.as('avai_port_green'),
                    avai_port: sql<number>`b.avai_port`.as('avai_port'),

                    amount_port_black: sql<number>`COALESCE(b.amount_port_black, 0)`.as('amount_port_black'),
                    amount_port_red: sql<number>`b.amount_port_red`.as('amount_port_red'),
                    amount_port_yellow: sql<number>`b.amount_port_yellow`.as('amount_port_yellow'),
                    amount_port_green: sql<number>`b.amount_port_green`.as('amount_port_green'),
                    amount_port: sql<number>`b.amount_port`.as('amount_port'),

                    amount_port_black_m1: sql<number>`COALESCE(c.amount_port_black, 0)`.as('amount_port_black_m1'),
                    amount_port_red_m1: sql<number>`c.amount_port_red`.as('amount_port_red_m1'),
                    amount_port_yellow_m1: sql<number>`c.amount_port_yellow`.as('amount_port_yellow_m1'),
                    amount_port_green_m1: sql<number>`c.amount_port_green`.as('amount_port_green_m1'),
                    amount_port_m1: sql<number>`c.amount_port`.as('amount_port_m1'),

                    total_odp_black: sql<number>`COALESCE(b.total_odp_black, 0)`.as('total_odp_black'),
                    total_odp_red: sql<number>`b.total_odp_red`.as('total_odp_red'),
                    total_odp_yellow: sql<number>`b.total_odp_yellow`.as('total_odp_yellow'),
                    total_odp_green: sql<number>`b.total_odp_green`.as('total_odp_green'),
                    total_odp: sql<number>`b.total_odp`.as('total_odp'),

                    golive_m: sql<number>`d.golive_m`.as('golive_m'),
                    golive_m1: sql<number>`d.golive_m1`.as('golive_m1'),
                    golive_mom: sql<string>`d.golive_mom`.as('golive_mom'),
                    golive_y: sql<number>`d.golive_y`.as('golive_y'),
                    golive_y1: sql<number>`d.golive_y1`.as('golive_y1'),
                    golive_yoy: sql<string>`d.golive_yoy`.as('golive_yoy'),
                    golive_ytd: sql<number>`d.golive_ytd`.as('golive_ytd'),
                    golive_ytd1: sql<number>`d.golive_ytd1`.as('golive_ytd1'),
                    golive_ytd_per: sql<string>`d.golive_ytd_per`.as('golive_ytd_per'),

                    target_ytd_demand: sql<number>`t.ytd_demands`.as('target_ytd_demand'),
                    demand_created_mtd: sql<number>`COALESCE(e.demand_created_mtd, 0)`.as('demand_created_mtd'),
                    demand_created_m1: sql<number>`COALESCE(e.demand_created_m1, 0)`.as('demand_created_m1'),
                    demand_created_mom: sql<string>`CONCAT(COALESCE(ROUND((e.demand_created_mtd - e.demand_created_m1) / e.demand_created_m1 * 100, 2), 0), '%')`.as('demand_created_mom'),
                    ach_demands: sql<string>`CONCAT(COALESCE(ROUND(e.demand_created_mtd / t.ytd_demands * 100, 2) ,0), '%')`.as('ach_demands'),

                    avai_port_1mo_y: sql<number>`f.avai_port_1mo_y`.as('avai_port_1mo_y'),
                    avai_port_2mo_y: sql<number>`f.avai_port_2mo_y`.as('avai_port_2mo_y'),
                    avai_port_3mo_y: sql<number>`f.avai_port_3mo_y`.as('avai_port_3mo_y'),
                    avai_port_4mo_y: sql<number>`f.avai_port_4mo_y`.as('avai_port_4mo_y'),

                    used_1mo_y: sql<number>`f.used_1mo_y`.as('used_1mo_y'),
                    used_2mo_y: sql<number>`f.used_2mo_y`.as('used_2mo_y'),
                    used_3mo_y: sql<number>`f.used_3mo_y`.as('used_3mo_y'),
                    used_4mo_y: sql<number>`f.used_4mo_y`.as('used_4mo_y'),
                    used_gt_6mo_y: sql<number>`f.used_gt_6mo_y`.as('used_gt_6mo_y'),
                    used_all_mo_y: sql<number>`f.used_all_mo_y`.as('used_all_mo_y'),

                    amount_port_1mo_y: sql<number>`f.amount_port_1mo_y`.as('amount_port_1mo_y'),
                    amount_port_2mo_y: sql<number>`f.amount_port_2mo_y`.as('amount_port_2mo_y'),
                    amount_port_3mo_y: sql<number>`f.amount_port_3mo_y`.as('amount_port_3mo_y'),
                    amount_port_4mo_y: sql<number>`f.amount_port_4mo_y`.as('amount_port_4mo_y'),
                    amount_port_gt_6mo_y: sql<number>`f.amount_port_gt_6mo_y`.as('amount_port_gt_6mo_y'),
                    amount_port_all_mo_y: sql<number>`f.amount_port_all_mo_y`.as('amount_port_all_mo_y'),

                    used_1mo_y1: sql<number>`f.used_1mo_y1`.as('used_1mo_y1'),
                    used_2mo_y1: sql<number>`f.used_2mo_y1`.as('used_2mo_y1'),
                    used_3mo_y1: sql<number>`f.used_3mo_y1`.as('used_3mo_y1'),
                    used_4mo_y1: sql<number>`f.used_4mo_y1`.as('used_4mo_y1'),
                    used_gt_6mo_y1: sql<number>`f.used_gt_6mo_y1`.as('used_gt_6mo_y1'),
                    used_all_mo_y1: sql<number>`f.used_all_mo_y1`.as('used_all_mo_y1'),

                    amount_port_1mo_y1: sql<number>`f.amount_port_1mo_y1`.as('amount_port_1mo_y1'),
                    amount_port_2mo_y1: sql<number>`f.amount_port_2mo_y1`.as('amount_port_2mo_y1'),
                    amount_port_3mo_y1: sql<number>`f.amount_port_3mo_y1`.as('amount_port_3mo_y1'),
                    amount_port_4mo_y1: sql<number>`f.amount_port_4mo_y1`.as('amount_port_4mo_y1'),
                    amount_port_gt_6mo_y1: sql<number>`f.amount_port_gt_6mo_y1`.as('amount_port_gt_6mo_y1'),
                    amount_port_all_mo_y1: sql<number>`f.amount_port_all_mo_y1`.as('amount_port_all_mo_y1'),

                    occ_1mo_y: sql<string>`f.occ_1mo_y`.as('occ_1mo_y'),
                    occ_2mo_y: sql<string>`f.occ_2mo_y`.as('occ_2mo_y'),
                    occ_3mo_y: sql<string>`f.occ_3mo_y`.as('occ_3mo_y'),
                    occ_4mo_y: sql<string>`f.occ_4mo_y`.as('occ_4mo_y'),
                    occ_gt_6mo_y: sql<string>`f.occ_gt_6mo_y`.as('occ_gt_6mo_y'),
                    occ_all_mo_y: sql<string>`f.occ_all_mo_y`.as('occ_all_mo_y'),

                    occ_1mo_y1: sql<string>`f.occ_1mo_y1`.as('occ_1mo_y1'),
                    occ_2mo_y1: sql<string>`f.occ_2mo_y1`.as('occ_2mo_y1'),
                    occ_3mo_y1: sql<string>`f.occ_3mo_y1`.as('occ_3mo_y1'),
                    occ_4mo_y1: sql<string>`f.occ_4mo_y1`.as('occ_4mo_y1'),
                    occ_gt_6mo_y1: sql<string>`f.occ_gt_6mo_y1`.as('occ_gt_6mo_y1'),
                    occ_all_mo_y1: sql<string>`f.occ_all_mo_y1`.as('occ_all_mo_y1'),

                    occ_1mo_2y: sql<string>`CONCAT(ROUND((f.used_1mo_y + f.used_1mo_y1) / (f.amount_port_1mo_y + f.amount_port_1mo_y1) * 100 ,2), '%')`.as('occ_1mo_2y'),
                    occ_2mo_2y: sql<string>`CONCAT(ROUND((f.used_2mo_y + f.used_2mo_y1) / (f.amount_port_2mo_y + f.amount_port_2mo_y1) * 100 ,2), '%')`.as('occ_2mo_2y'),
                    occ_3mo_2y: sql<string>`CONCAT(ROUND((f.used_3mo_y + f.used_3mo_y1) / (f.amount_port_3mo_y + f.amount_port_3mo_y1) * 100 ,2), '%')`.as('occ_3mo_2y'),
                    occ_4mo_2y: sql<string>`CONCAT(ROUND((f.used_4mo_y + f.used_4mo_y1) / (f.amount_port_4mo_y + f.amount_port_4mo_y1) * 100 ,2), '%')`.as('occ_4mo_2y'),
                    occ_gt_6mo_2y: sql<string>`CONCAT(ROUND((f.used_gt_6mo_y + f.used_gt_6mo_y1) / (f.amount_port_gt_6mo_y + f.amount_port_gt_6mo_y1) * 100 ,2), '%')`.as('occ_gt_6mo_2y'),
                    occ_all_mo_2y: sql<string>`CONCAT(ROUND((f.used_all_mo_y + f.used_all_mo_y1) / (f.amount_port_all_mo_y + f.amount_port_all_mo_y1) * 100 ,2), '%')`.as('occ_all_mo_2y'),
                })
                .from(branchTerritory)
                .leftJoin(branchCurrOdp, sql`${branchTerritory.branch} = b.branch`)
                .leftJoin(branchPrevOdp, sql`${branchTerritory.branch} = c.branch`)
                .leftJoin(branchGoLive, sql`${branchTerritory.branch} = d.branch`)
                .leftJoin(branchDemands, sql`${branchTerritory.branch} = e.branch`)
                .leftJoin(branchGoLiveCurrYear, sql`${branchTerritory.branch} = f.branch`)
                .leftJoin(branchTargetDemands, sql`${branchTerritory.branch} = t.branch`)
                .groupBy(sql`1,2`)

            const wokTerritory = db
                .select({ wok: territoryHousehold.wok })
                .from(territoryHousehold)
                .where(branch && wok ? and(
                    eq(territoryHousehold.regional, 'MALUKU DAN PAPUA'),
                    eq(territoryHousehold.branch, branch),
                    eq(territoryHousehold.wok, wok)
                ) : eq(territoryHousehold.regional, 'MALUKU DAN PAPUA'))
                .groupBy(territoryHousehold.wok)
                .as('a')

            const wokCurrOdp = db
                .select({
                    wok: sql<string>`bb.wok`.as('wok'),
                    used_black: sql<number>`SUM(CASE WHEN status = 'BLACK' THEN used END)`.as('used_black'),
                    used_red: sql<number>`SUM(CASE WHEN status = 'RED' THEN used END)`.as('used_red'),
                    used_yellow: sql<number>`SUM(CASE WHEN status = 'YELLOW' THEN used END)`.as('used_yellow'),
                    used_green: sql<number>`SUM(CASE WHEN status = 'GREEN' THEN used END)`.as('used_green'),
                    used: sql<number>`SUM(used)`.as('used'),

                    avai_port_black: sql<number>`SUM(CASE WHEN status = 'BLACK' THEN avai_port END)`.as('avai_port_black'),
                    avai_port_red: sql<number>`SUM(CASE WHEN status = 'RED' THEN avai_port END)`.as('avai_port_red'),
                    avai_port_yellow: sql<number>`SUM(CASE WHEN status = 'YELLOW' THEN avai_port END)`.as('avai_port_yellow'),
                    avai_port_green: sql<number>`SUM(CASE WHEN status = 'GREEN' THEN avai_port END)`.as('avai_port_green'),
                    avai_port: sql<number>`SUM(avai_port)`.as('avai_port'),

                    amount_port_black: sql<number>`SUM(CASE WHEN status = 'BLACK' THEN amount_port END)`.as('amount_port_black'),
                    amount_port_red: sql<number>`SUM(CASE WHEN status = 'RED' THEN amount_port END)`.as('amount_port_red'),
                    amount_port_yellow: sql<number>`SUM(CASE WHEN status = 'YELLOW' THEN amount_port END)`.as('amount_port_yellow'),
                    amount_port_green: sql<number>`SUM(CASE WHEN status = 'GREEN' THEN amount_port END)`.as('amount_port_green'),
                    amount_port: sql<number>`SUM(amount_port)`.as('amount_port'),

                    total_odp_black: sql<number>`SUM(CASE WHEN status = 'BLACK' THEN odp END)`.as('total_odp_black'),
                    total_odp_red: sql<number>`SUM(CASE WHEN status = 'RED' THEN odp END)`.as('total_odp_red'),
                    total_odp_yellow: sql<number>`SUM(CASE WHEN status = 'YELLOW' THEN odp END)`.as('total_odp_yellow'),
                    total_odp_green: sql<number>`SUM(CASE WHEN status = 'GREEN' THEN odp END)`.as('total_odp_green'),
                    total_odp: sql<number>`SUM(bb.odp)`.as('total_odp')
                })
                .from(db
                    .select({
                        wok: sql<string>`
                    CASE
                        WHEN sto IN ('WHA','TUA','SPR','SML','NML','NIR','MSH','LRA','DOB','BUL') THEN 'AMBON OUTER'
                        WHEN sto IN ('PAO', 'ABO') THEN 'AMBON INNER'
                        WHEN sto IN ('WAM','SRU','SRM','SNI','BIA') THEN 'JAYAPURA OUTER'
                        WHEN sto IN ('WAE','JPB','JAP','ABE') THEN 'JAYAPURA INNER'
                        WHEN sto IN ('TIM','TBG','KUK','NAB') THEN 'MIMIKA'
                        WHEN sto IN ('TMR','MRK','BAD','AGT') THEN 'MERAUKE'
                        WHEN sto IN ('WMR','RSK','MWR','KIN','FFA','BTI') THEN 'MANOKWARI NABIRE'
                        WHEN sto IN ('TMB', 'SON') THEN 'SORONG RAJA AMPAT'
                    END`.as('wok'),
                        status: sql<string>`
                    CASE
                        WHEN ${currOdpTable.status} IN ('BLACK', 'BLACKSYSTEM') THEN 'BLACK' ELSE ${currOdpTable.status}
                    END`.as('status'),
                        amount_port: sum(currOdpTable.is_total).as('amount_port'),
                        avai_port: sum(currOdpTable.avai).as('avai_port'),
                        used: sum(currOdpTable.used).as('used'),
                        odp: count(currOdpTable.status).as('odp')
                    })
                    .from(currOdpTable)
                    .where(and(
                        inArray(currOdpTable.witel, ['MALUKU', 'PAPUA', 'PAPUA BARAT']),
                        eq(currOdpTable.event_date, currDate)
                    ))
                    .groupBy(sql`1,2`)
                    .as('bb'))
                .groupBy(sql`1`)
                .as('b')

            const wokPrevOdp = db
                .select({
                    wok: sql<string>`cc.wok`.as('wok'),
                    used_black: sql<number>`SUM(CASE WHEN status = 'BLACK' THEN used END)`.as('used_black'),
                    used_red: sql<number>`SUM(CASE WHEN status = 'RED' THEN used END)`.as('used_red'),
                    used_yellow: sql<number>`SUM(CASE WHEN status = 'YELLOW' THEN used END)`.as('used_yellow'),
                    used_green: sql<number>`SUM(CASE WHEN status = 'GREEN' THEN used END)`.as('used_green'),
                    used: sql<number>`SUM(used)`.as('used'),

                    amount_port_black: sql<number>`SUM(CASE WHEN status = 'BLACK' THEN amount_port END)`.as('amount_port_black'),
                    amount_port_red: sql<number>`SUM(CASE WHEN status = 'RED' THEN amount_port END)`.as('amount_port_red'),
                    amount_port_yellow: sql<number>`SUM(CASE WHEN status = 'YELLOW' THEN amount_port END)`.as('amount_port_yellow'),
                    amount_port_green: sql<number>`SUM(CASE WHEN status = 'GREEN' THEN amount_port END)`.as('amount_port_green'),
                    amount_port: sql<number>`SUM(amount_port)`.as('amount_port'),
                })
                .from(db
                    .select({
                        wok: sql<string>`
                    CASE
                        WHEN sto IN ('WHA','TUA','SPR','SML','NML','NIR','MSH','LRA','DOB','BUL') THEN 'AMBON OUTER'
                        WHEN sto IN ('PAO', 'ABO') THEN 'AMBON INNER'
                        WHEN sto IN ('WAM','SRU','SRM','SNI','BIA') THEN 'JAYAPURA OUTER'
                        WHEN sto IN ('WAE','JPB','JAP','ABE') THEN 'JAYAPURA INNER'
                        WHEN sto IN ('TIM','TBG','KUK','NAB') THEN 'MIMIKA'
                        WHEN sto IN ('TMR','MRK','BAD','AGT') THEN 'MERAUKE'
                        WHEN sto IN ('WMR','RSK','MWR','KIN','FFA','BTI') THEN 'MANOKWARI NABIRE'
                        WHEN sto IN ('TMB', 'SON') THEN 'SORONG RAJA AMPAT'
                    END`.as('wok'),
                        status: sql<string>`
                    CASE
                        WHEN ${prevOdpTable.status} IN ('BLACK', 'BLACKSYSTEM') THEN 'BLACK' ELSE ${prevOdpTable.status}
                    END`.as('status'),
                        used: sum(prevOdpTable.used).as('used'),
                        amount_port: sum(prevOdpTable.is_total).as('amount_port')
                    })
                    .from(prevOdpTable)
                    .where(and(
                        inArray(prevOdpTable.witel, ['MALUKU', 'PAPUA', 'PAPUA BARAT']),
                        eq(prevOdpTable.event_date, prevDate)
                    ))
                    .groupBy(sql`1,2`)
                    .as('cc')
                )
                .groupBy(sql`1`)
                .as('c')

            const wokGoLive = db
                .select({
                    wok: ih_occ_golive_ihld.wok,
                    golive_m: sql<number>`COUNT(CASE WHEN ${ih_occ_golive_ihld.tanggal_go_live_uim} BETWEEN ${currStartOfMonth} AND ${currDate} THEN 1 END)`.as('golive_m'),
                    golive_m1: sql<number>`COUNT(CASE WHEN ${ih_occ_golive_ihld.tanggal_go_live_uim} BETWEEN ${prevStartOfMonth} AND ${prevDate2} THEN 1 END)`.as('golive_m1'),

                    // Add y and y1 calculations here directly
                    golive_y: sql<number>`COUNT(CASE WHEN tahun_go_live_uim = 2025 THEN 1 END)`.as('golive_y'),
                    golive_y1: sql<number>`COUNT(CASE WHEN tahun_go_live_uim = 2024 THEN 1 END)`.as('golive_y1'),

                    golive_ytd: sql<number>`COUNT(CASE WHEN ${ih_occ_golive_ihld.tanggal_go_live_uim} BETWEEN ${currJanuaryFirst} AND ${currDate} THEN 1 END)`.as('golive_ytd'),
                    golive_ytd1: sql<number>`COUNT(CASE WHEN ${ih_occ_golive_ihld.tanggal_go_live_uim} BETWEEN ${prevJanuaryFirst} AND ${prevYearCurrDate} THEN 1 END)`.as('golive_ytd1'),

                    // Calculate MoM and YoY percentages here as well
                    golive_mom: sql<string>`CONCAT(COALESCE(ROUND((COUNT(CASE WHEN ${ih_occ_golive_ihld.tanggal_go_live_uim} BETWEEN ${currStartOfMonth} AND ${currDate} THEN 1 END) - COUNT(CASE WHEN ${ih_occ_golive_ihld.tanggal_go_live_uim} BETWEEN ${prevStartOfMonth} AND ${prevDate2} THEN 1 END)) / COUNT(CASE WHEN ${ih_occ_golive_ihld.tanggal_go_live_uim} BETWEEN ${prevStartOfMonth} AND ${prevDate2} THEN 1 END) * 100, 2),0), '%')`.as('golive_mom'),
                    golive_yoy: sql<string>`CONCAT(COALESCE(ROUND((COUNT(CASE WHEN tahun_go_live_uim = 2025 THEN 1 END) - COUNT(CASE WHEN tahun_go_live_uim = 2024 THEN 1 END)) / COUNT(CASE WHEN tahun_go_live_uim = 2024 THEN 1 END) * 100, 2),0), '%')`.as('golive_yoy'),
                    golive_ytd_per: sql<string>`CONCAT(COALESCE(ROUND((COUNT(CASE WHEN ${ih_occ_golive_ihld.tanggal_go_live_uim} BETWEEN ${currJanuaryFirst} AND ${currDate} THEN 1 END) - COUNT(CASE WHEN ${ih_occ_golive_ihld.tanggal_go_live_uim} BETWEEN ${prevJanuaryFirst} AND ${prevYearCurrDate} THEN 1 END)) / COUNT(CASE WHEN ${ih_occ_golive_ihld.tanggal_go_live_uim} BETWEEN ${prevJanuaryFirst} AND ${prevYearCurrDate} THEN 1 END) * 100, 2),0), '%')`.as('golive_ytd_per')
                })
                .from(ih_occ_golive_ihld)
                .groupBy(ih_occ_golive_ihld.wok)
                .as('d')

            const wokDemands = db
                .select({
                    wok: ih_demand_mysiis.wok,
                    demand_created_mtd: sql<number>`SUM(CASE WHEN DATE_FORMAT(approved_at, '%Y-%m-%d') BETWEEN ${currStartOfMonth} AND ${currDate} THEN ${ih_demand_mysiis.target} END)`.as('demand_created_mtd'),
                    demand_created_m1: sql<number>`SUM(CASE WHEN DATE_FORMAT(approved_at, '%Y-%m-%d') BETWEEN ${prevStartOfMonth} AND ${prevMonthSameDate} THEN ${ih_demand_mysiis.target} END)`.as('demand_created_m1')
                })
                .from(ih_demand_mysiis)
                .where(and(
                    gt(ih_demand_mysiis.target, 0),
                    eq(ih_demand_mysiis.region, 'MALUKU DAN PAPUA'),
                    inArray(ih_demand_mysiis.status_new_bispro, ['Wait Approval AI', 'Draft']),
                    sql`YEAR(${ih_demand_mysiis.approved_at}) = ${currYear}`
                ))
                .groupBy(sql`1`)
                .as('e')

            const wokGoLiveCurrYear = db
                .select({
                    wok: sql<string>`ff.wok`.as('wok'),

                    avai_port_1mo_y: sql<number>`COALESCE(ff.avai_port_1mo_y, 0)`.as('avai_port_1mo_y'),
                    avai_port_2mo_y: sql<number>`COALESCE(ff.avai_port_2mo_y, 0)`.as('avai_port_2mo_y'),
                    avai_port_3mo_y: sql<number>`COALESCE(ff.avai_port_3mo_y, 0)`.as('avai_port_3mo_y'),
                    avai_port_4mo_y: sql<number>`COALESCE(ff.avai_port_4mo_y, 0)`.as('avai_port_4mo_y'),

                    used_1mo_y: sql<number>`COALESCE(ff.used_1mo_y, 0)`.as('used_1mo_y'),
                    used_2mo_y: sql<number>`COALESCE(ff.used_2mo_y, 0)`.as('used_2mo_y'),
                    used_3mo_y: sql<number>`COALESCE(ff.used_3mo_y, 0)`.as('used_3mo_y'),
                    used_4mo_y: sql<number>`COALESCE(ff.used_4mo_y, 0)`.as('used_4mo_y'),
                    used_gt_6mo_y: sql<number>`COALESCE(ff.used_gt_6mo_y, 0)`.as('used_gt_6mo_y'),
                    used_all_mo_y: sql<number>`COALESCE(ff.used_all_mo_y, 0)`.as('used_all_mo_y'),

                    amount_port_1mo_y: sql<number>`COALESCE(ff.amount_port_1mo_y, 0)`.as('amount_port_1mo_y'),
                    amount_port_2mo_y: sql<number>`COALESCE(ff.amount_port_2mo_y, 0)`.as('amount_port_2mo_y'),
                    amount_port_3mo_y: sql<number>`COALESCE(ff.amount_port_3mo_y, 0)`.as('amount_port_3mo_y'),
                    amount_port_4mo_y: sql<number>`COALESCE(ff.amount_port_4mo_y, 0)`.as('amount_port_4mo_y'),
                    amount_port_gt_6mo_y: sql<number>`COALESCE(ff.amount_port_gt_6mo_y, 0)`.as('amount_port_gt_6mo_y'),
                    amount_port_all_mo_y: sql<number>`COALESCE(ff.amount_port_all_mo_y, 0)`.as('amount_port_all_mo_y'),

                    used_1mo_y1: sql<number>`COALESCE(ff.used_1mo_y1, 0)`.as('used_1mo_y1'),
                    used_2mo_y1: sql<number>`COALESCE(ff.used_2mo_y1, 0)`.as('used_2mo_y1'),
                    used_3mo_y1: sql<number>`COALESCE(ff.used_3mo_y1, 0)`.as('used_3mo_y1'),
                    used_4mo_y1: sql<number>`COALESCE(ff.used_4mo_y1, 0)`.as('used_4mo_y1'),
                    used_gt_6mo_y1: sql<number>`COALESCE(ff.used_gt_6mo_y1, 0)`.as('used_gt_6mo_y1'),
                    used_all_mo_y1: sql<number>`COALESCE(ff.used_all_mo_y1, 0)`.as('used_all_mo_y1'),

                    amount_port_1mo_y1: sql<number>`COALESCE(ff.amount_port_1mo_y1, 0)`.as('amount_port_1mo_y1'),
                    amount_port_2mo_y1: sql<number>`COALESCE(ff.amount_port_2mo_y1, 0)`.as('amount_port_2mo_y1'),
                    amount_port_3mo_y1: sql<number>`COALESCE(ff.amount_port_3mo_y1, 0)`.as('amount_port_3mo_y1'),
                    amount_port_4mo_y1: sql<number>`COALESCE(ff.amount_port_4mo_y1, 0)`.as('amount_port_4mo_y1'),
                    amount_port_gt_6mo_y1: sql<number>`COALESCE(ff.amount_port_gt_6mo_y1, 0)`.as('amount_port_gt_6mo_y1'),
                    amount_port_all_mo_y1: sql<number>`COALESCE(ff.amount_port_all_mo_y1, 0)`.as('amount_port_all_mo_y1'),

                    occ_1mo_y: sql<string>`CONCAT(COALESCE(ROUND(SUM(used_1mo_y) / SUM(amount_port_1mo_y) * 100, 2), 0), '%')`.as('occ_1mo_y'),
                    occ_2mo_y: sql<string>`CONCAT(COALESCE(ROUND(SUM(used_2mo_y) / SUM(amount_port_2mo_y) * 100, 2), 0), '%')`.as('occ_2mo_y'),
                    occ_3mo_y: sql<string>`CONCAT(COALESCE(ROUND(SUM(used_3mo_y) / SUM(amount_port_3mo_y) * 100, 2), 0), '%')`.as('occ_3mo_y'),
                    occ_4mo_y: sql<string>`CONCAT(COALESCE(ROUND(SUM(used_4mo_y) / SUM(amount_port_4mo_y) * 100, 2), 0), '%')`.as('occ_4mo_y'),
                    occ_gt_6mo_y: sql<string>`CONCAT(COALESCE(ROUND(SUM(used_gt_6mo_y) / SUM(amount_port_gt_6mo_y) * 100, 2), 0), '%')`.as('occ_gt_6mo_y'),
                    occ_all_mo_y: sql<string>`CONCAT(COALESCE(ROUND(SUM(used_all_mo_y) / SUM(amount_port_all_mo_y) * 100, 2), 0), '%')`.as('occ_all_mo_y'),

                    occ_1mo_y1: sql<string>`CONCAT(COALESCE(ROUND(SUM(used_1mo_y1) / SUM(amount_port_1mo_y1) * 100, 2), 0), '%')`.as('occ_1mo_y1'),
                    occ_2mo_y1: sql<string>`CONCAT(COALESCE(ROUND(SUM(used_2mo_y1) / SUM(amount_port_2mo_y1) * 100, 2), 0), '%')`.as('occ_2mo_y1'),
                    occ_3mo_y1: sql<string>`CONCAT(COALESCE(ROUND(SUM(used_3mo_y1) / SUM(amount_port_3mo_y1) * 100, 2), 0), '%')`.as('occ_3mo_y1'),
                    occ_4mo_y1: sql<string>`CONCAT(COALESCE(ROUND(SUM(used_4mo_y1) / SUM(amount_port_4mo_y1) * 100, 2), 0), '%')`.as('occ_4mo_y1'),
                    occ_gt_6mo_y1: sql<string>`CONCAT(COALESCE(ROUND(SUM(used_gt_6mo_y1) / SUM(amount_port_gt_6mo_y1) * 100, 2), 0), '%')`.as('occ_gt_6mo_y1'),
                    occ_all_mo_y1: sql<string>`CONCAT(COALESCE(ROUND(SUM(used_all_mo_y1) / SUM(amount_port_all_mo_y1) * 100, 2), 0), '%')`.as('occ_all_mo_y1'),
                })
                .from(db
                    .select({
                        wok: ih_occ_golive_ihld.wok,

                        avai_port_1mo_y: sql<number>`SUM(CASE WHEN DATEDIFF((SELECT MAX(tanggal_go_live_uim) FROM household.ih_occ_golive_ihld), tanggal_go_live_uim) <= 30 AND YEAR(tanggal_go_live_uim) = ${currYear} THEN avai END)`.as('avai_port_1mo_y'),
                        avai_port_2mo_y: sql<number>`SUM(CASE WHEN DATEDIFF((SELECT MAX(tanggal_go_live_uim) FROM household.ih_occ_golive_ihld), tanggal_go_live_uim) <= 60 AND YEAR(tanggal_go_live_uim) = ${currYear} THEN avai END)`.as('avai_port_2mo_y'),
                        avai_port_3mo_y: sql<number>`SUM(CASE WHEN DATEDIFF((SELECT MAX(tanggal_go_live_uim) FROM household.ih_occ_golive_ihld), tanggal_go_live_uim) <= 90 AND YEAR(tanggal_go_live_uim) = ${currYear} THEN avai END)`.as('avai_port_3mo_y'),
                        avai_port_4mo_y: sql<number>`SUM(CASE WHEN DATEDIFF((SELECT MAX(tanggal_go_live_uim) FROM household.ih_occ_golive_ihld), tanggal_go_live_uim) <= 180 AND YEAR(tanggal_go_live_uim) = ${currYear} THEN avai END)`.as('avai_port_4mo_y'),

                        used_1mo_y: sql<number>`SUM(CASE WHEN DATEDIFF((SELECT MAX(tanggal_go_live_uim) FROM household.ih_occ_golive_ihld), tanggal_go_live_uim) <= 30 AND YEAR(tanggal_go_live_uim) = ${currYear} THEN used END)`.as('used_1mo_y'),
                        used_2mo_y: sql<number>`SUM(CASE WHEN DATEDIFF((SELECT MAX(tanggal_go_live_uim) FROM household.ih_occ_golive_ihld), tanggal_go_live_uim) <= 60 AND YEAR(tanggal_go_live_uim) = ${currYear} THEN used END)`.as('used_2mo_y'),
                        used_3mo_y: sql<number>`SUM(CASE WHEN DATEDIFF((SELECT MAX(tanggal_go_live_uim) FROM household.ih_occ_golive_ihld), tanggal_go_live_uim) <= 90 AND YEAR(tanggal_go_live_uim) = ${currYear} THEN used END)`.as('used_3mo_y'),
                        used_4mo_y: sql<number>`SUM(CASE WHEN DATEDIFF((SELECT MAX(tanggal_go_live_uim) FROM household.ih_occ_golive_ihld), tanggal_go_live_uim) <= 180 AND YEAR(tanggal_go_live_uim) = ${currYear} THEN used END)`.as('used_4mo_y'),
                        used_gt_6mo_y: sql<number>`SUM(CASE WHEN DATEDIFF((SELECT MAX(tanggal_go_live_uim) FROM household.ih_occ_golive_ihld), tanggal_go_live_uim) > 180 AND YEAR(tanggal_go_live_uim) = ${currYear} THEN used END)`.as('used_gt_6mo_y'),
                        used_all_mo_y: sql<number>`SUM(CASE WHEN YEAR(tanggal_go_live_uim) = ${currYear} THEN used END)`.as('used_all_mo_y'),

                        used_1mo_y1: sql<number>`SUM(CASE WHEN DATEDIFF((SELECT MAX(tanggal_go_live_uim) FROM household.ih_occ_golive_ihld), tanggal_go_live_uim) <= 30 AND YEAR(tanggal_go_live_uim) = ${prevYear} THEN used END)`.as('used_1mo_y1'),
                        used_2mo_y1: sql<number>`SUM(CASE WHEN DATEDIFF((SELECT MAX(tanggal_go_live_uim) FROM household.ih_occ_golive_ihld), tanggal_go_live_uim) <= 60 AND YEAR(tanggal_go_live_uim) = ${prevYear} THEN used END)`.as('used_2mo_y1'),
                        used_3mo_y1: sql<number>`SUM(CASE WHEN DATEDIFF((SELECT MAX(tanggal_go_live_uim) FROM household.ih_occ_golive_ihld), tanggal_go_live_uim) <= 90 AND YEAR(tanggal_go_live_uim) = ${prevYear} THEN used END)`.as('used_3mo_y1'),
                        used_4mo_y1: sql<number>`SUM(CASE WHEN DATEDIFF((SELECT MAX(tanggal_go_live_uim) FROM household.ih_occ_golive_ihld), tanggal_go_live_uim) <= 180 AND YEAR(tanggal_go_live_uim) = ${prevYear} THEN used END)`.as('used_4mo_y1'),
                        used_gt_6mo_y1: sql<number>`SUM(CASE WHEN DATEDIFF((SELECT MAX(tanggal_go_live_uim) FROM household.ih_occ_golive_ihld), tanggal_go_live_uim) > 180 AND YEAR(tanggal_go_live_uim) = ${prevYear} THEN used END)`.as('used_gt_6mo_y1'),
                        used_all_mo_y1: sql<number>`SUM(CASE WHEN YEAR(tanggal_go_live_uim) = ${prevYear} THEN used END)`.as('used_all_mo_y1'),

                        amount_port_1mo_y: sql<number>`SUM(CASE WHEN DATEDIFF((SELECT MAX(tanggal_go_live_uim) FROM household.ih_occ_golive_ihld), tanggal_go_live_uim) <= 30 AND YEAR(tanggal_go_live_uim) = ${currYear} THEN is_total END)`.as('amount_port_1mo_y'),
                        amount_port_2mo_y: sql<number>`SUM(CASE WHEN DATEDIFF((SELECT MAX(tanggal_go_live_uim) FROM household.ih_occ_golive_ihld), tanggal_go_live_uim) <= 60 AND YEAR(tanggal_go_live_uim) = ${currYear} THEN is_total END)`.as('amount_port_2mo_y'),
                        amount_port_3mo_y: sql<number>`SUM(CASE WHEN DATEDIFF((SELECT MAX(tanggal_go_live_uim) FROM household.ih_occ_golive_ihld), tanggal_go_live_uim) <= 90 AND YEAR(tanggal_go_live_uim) = ${currYear} THEN is_total END)`.as('amount_port_3mo_y'),
                        amount_port_4mo_y: sql<number>`SUM(CASE WHEN DATEDIFF((SELECT MAX(tanggal_go_live_uim) FROM household.ih_occ_golive_ihld), tanggal_go_live_uim) <= 180 AND YEAR(tanggal_go_live_uim) = ${currYear} THEN is_total END)`.as('amount_port_4mo_y'),
                        amount_port_gt_6mo_y: sql<number>`SUM(CASE WHEN DATEDIFF((SELECT MAX(tanggal_go_live_uim) FROM household.ih_occ_golive_ihld), tanggal_go_live_uim) > 180 AND YEAR(tanggal_go_live_uim) = ${currYear} THEN is_total END)`.as('amount_port_gt_6mo_y'),
                        amount_port_all_mo_y: sql<number>`SUM(CASE WHEN YEAR(tanggal_go_live_uim) = ${currYear} THEN is_total END)`.as('amount_port_all_mo_y'),

                        amount_port_1mo_y1: sql<number>`SUM(CASE WHEN DATEDIFF((SELECT MAX(tanggal_go_live_uim) FROM household.ih_occ_golive_ihld), tanggal_go_live_uim) <= 30 AND YEAR(tanggal_go_live_uim) = ${prevYear} THEN is_total END)`.as('amount_port_1mo_y1'),
                        amount_port_2mo_y1: sql<number>`SUM(CASE WHEN DATEDIFF((SELECT MAX(tanggal_go_live_uim) FROM household.ih_occ_golive_ihld), tanggal_go_live_uim) <= 60 AND YEAR(tanggal_go_live_uim) = ${prevYear} THEN is_total END)`.as('amount_port_2mo_y1'),
                        amount_port_3mo_y1: sql<number>`SUM(CASE WHEN DATEDIFF((SELECT MAX(tanggal_go_live_uim) FROM household.ih_occ_golive_ihld), tanggal_go_live_uim) <= 90 AND YEAR(tanggal_go_live_uim) = ${prevYear} THEN is_total END)`.as('amount_port_3mo_y1'),
                        amount_port_4mo_y1: sql<number>`SUM(CASE WHEN DATEDIFF((SELECT MAX(tanggal_go_live_uim) FROM household.ih_occ_golive_ihld), tanggal_go_live_uim) <= 180 AND YEAR(tanggal_go_live_uim) = ${prevYear} THEN is_total END)`.as('amount_port_4mo_y1'),
                        amount_port_gt_6mo_y1: sql<number>`SUM(CASE WHEN DATEDIFF((SELECT MAX(tanggal_go_live_uim) FROM household.ih_occ_golive_ihld), tanggal_go_live_uim) > 180 AND YEAR(tanggal_go_live_uim) = ${prevYear} THEN is_total END)`.as('amount_port_gt_6mo_y1'),
                        amount_port_all_mo_y1: sql<number>`SUM(CASE WHEN YEAR(tanggal_go_live_uim) = ${prevYear} THEN is_total END)`.as('amount_port_all_mo_y1'),
                    })
                    .from(ih_occ_golive_ihld)
                    .where(and(
                        eq(ih_occ_golive_ihld.telkomsel_regional, 'MALUKU DAN PAPUA'),
                        sql`YEAR(tanggal_go_live_uim) = ${currYear}`
                    ))
                    .groupBy(sql`1`)
                    .as('ff')
                )
                .groupBy(sql`1`)
                .as('f')

            const wokTargetDemands = db
                .select({
                    wok: targetTerritoryDemands.wok,
                    ytd_demands: sum(targetTerritoryDemands.ytd_demand).as('ytd_demands')
                })
                .from(targetTerritoryDemands)
                .rightJoin(
                    db
                        .selectDistinct({ wok: territoryHousehold.wok })
                        .from(territoryHousehold)
                        .as('b'),
                    eq(targetTerritoryDemands.wok, sql`b.wok`)
                )
                .groupBy(sql`1`)
                .as('t')

            const wokFinalQuery = db
                .select({
                    name: wokTerritory.wok,
                    level: sql<string>`'wok'`.as('level'),

                    used_black: sql<number>`COALESCE(b.used_black, 0)`.as('used_black'),
                    used_red: sql<number>`b.used_red`.as('used_red'),
                    used_yellow: sql<number>`b.used_yellow`.as('used_yellow'),
                    used_green: sql<number>`b.used_green`.as('used_green'),
                    used: sql<number>`b.used`.as('used'),

                    used_black_m1: sql<number>`COALESCE(c.used_black, 0)`.as('used_black_m1'),
                    used_red_m1: sql<number>`c.used_red`.as('used_red_m1'),
                    used_yellow_m1: sql<number>`c.used_yellow`.as('used_yellow_m1'),
                    used_green_m1: sql<number>`c.used_green`.as('used_green_m1'),
                    used_m1: sql<number>`c.used`.as('used_m1'),

                    avai_port_black: sql<number>`COALESCE(b.avai_port_black, 0)`.as('avai_port_black'),
                    avai_port_red: sql<number>`b.avai_port_red`.as('avai_port_red'),
                    avai_port_yellow: sql<number>`b.avai_port_yellow`.as('avai_port_yellow'),
                    avai_port_green: sql<number>`b.avai_port_green`.as('avai_port_green'),
                    avai_port: sql<number>`b.avai_port`.as('avai_port'),

                    amount_port_black: sql<number>`COALESCE(b.amount_port_black, 0)`.as('amount_port_black'),
                    amount_port_red: sql<number>`b.amount_port_red`.as('amount_port_red'),
                    amount_port_yellow: sql<number>`b.amount_port_yellow`.as('amount_port_yellow'),
                    amount_port_green: sql<number>`b.amount_port_green`.as('amount_port_green'),
                    amount_port: sql<number>`b.amount_port`.as('amount_port'),

                    amount_port_black_m1: sql<number>`COALESCE(c.amount_port_black, 0)`.as('amount_port_black_m1'),
                    amount_port_red_m1: sql<number>`c.amount_port_red`.as('amount_port_red_m1'),
                    amount_port_yellow_m1: sql<number>`c.amount_port_yellow`.as('amount_port_yellow_m1'),
                    amount_port_green_m1: sql<number>`c.amount_port_green`.as('amount_port_green_m1'),
                    amount_port_m1: sql<number>`c.amount_port`.as('amount_port_m1'),

                    total_odp_black: sql<number>`COALESCE(b.total_odp_black, 0)`.as('total_odp_black'),
                    total_odp_red: sql<number>`b.total_odp_red`.as('total_odp_red'),
                    total_odp_yellow: sql<number>`b.total_odp_yellow`.as('total_odp_yellow'),
                    total_odp_green: sql<number>`b.total_odp_green`.as('total_odp_green'),
                    total_odp: sql<number>`b.total_odp`.as('total_odp'),

                    golive_m: sql<number>`d.golive_m`.as('golive_m'),
                    golive_m1: sql<number>`d.golive_m1`.as('golive_m1'),
                    golive_mom: sql<string>`d.golive_mom`.as('golive_mom'),
                    golive_y: sql<number>`d.golive_y`.as('golive_y'),
                    golive_y1: sql<number>`d.golive_y1`.as('golive_y1'),
                    golive_yoy: sql<string>`d.golive_yoy`.as('golive_yoy'),
                    golive_ytd: sql<number>`d.golive_ytd`.as('golive_ytd'),
                    golive_ytd1: sql<number>`d.golive_ytd1`.as('golive_ytd1'),
                    golive_ytd_per: sql<string>`d.golive_ytd_per`.as('golive_ytd_per'),

                    target_ytd_demand: sql<number>`t.ytd_demands`.as('target_ytd_demand'),
                    demand_created_mtd: sql<number>`COALESCE(e.demand_created_mtd, 0)`.as('demand_created_mtd'),
                    demand_created_m1: sql<number>`COALESCE(e.demand_created_m1, 0)`.as('demand_created_m1'),
                    demand_created_mom: sql<string>`CONCAT(ROUND((e.demand_created_mtd - e.demand_created_m1) / e.demand_created_m1 * 100, 2), '%')`.as('demand_created_mom'),
                    ach_demands: sql<string>`CONCAT(ROUND(e.demand_created_mtd / t.ytd_demands * 100 ,2), '%')`.as('ach_demands'),

                    avai_port_1mo_y: sql<number>`f.avai_port_1mo_y`.as('avai_port_1mo_y'),
                    avai_port_2mo_y: sql<number>`f.avai_port_2mo_y`.as('avai_port_2mo_y'),
                    avai_port_3mo_y: sql<number>`f.avai_port_3mo_y`.as('avai_port_3mo_y'),
                    avai_port_4mo_y: sql<number>`f.avai_port_4mo_y`.as('avai_port_4mo_y'),

                    used_1mo_y: sql<number>`f.used_1mo_y`.as('used_1mo_y'),
                    used_2mo_y: sql<number>`f.used_2mo_y`.as('used_2mo_y'),
                    used_3mo_y: sql<number>`f.used_3mo_y`.as('used_3mo_y'),
                    used_4mo_y: sql<number>`f.used_4mo_y`.as('used_4mo_y'),
                    used_gt_6mo_y: sql<number>`f.used_gt_6mo_y`.as('used_gt_6mo_y'),
                    used_all_mo_y: sql<number>`f.used_all_mo_y`.as('used_all_mo_y'),

                    amount_port_1mo_y: sql<number>`f.amount_port_1mo_y`.as('amount_port_1mo_y'),
                    amount_port_2mo_y: sql<number>`f.amount_port_2mo_y`.as('amount_port_2mo_y'),
                    amount_port_3mo_y: sql<number>`f.amount_port_3mo_y`.as('amount_port_3mo_y'),
                    amount_port_4mo_y: sql<number>`f.amount_port_4mo_y`.as('amount_port_4mo_y'),
                    amount_port_gt_6mo_y: sql<number>`f.amount_port_gt_6mo_y`.as('amount_port_gt_6mo_y'),
                    amount_port_all_mo_y: sql<number>`f.amount_port_all_mo_y`.as('amount_port_all_mo_y'),

                    used_1mo_y1: sql<number>`f.used_1mo_y1`.as('used_1mo_y1'),
                    used_2mo_y1: sql<number>`f.used_2mo_y1`.as('used_2mo_y1'),
                    used_3mo_y1: sql<number>`f.used_3mo_y1`.as('used_3mo_y1'),
                    used_4mo_y1: sql<number>`f.used_4mo_y1`.as('used_4mo_y1'),
                    used_gt_6mo_y1: sql<number>`f.used_gt_6mo_y1`.as('used_gt_6mo_y1'),
                    used_all_mo_y1: sql<number>`f.used_all_mo_y1`.as('used_all_mo_y1'),

                    amount_port_1mo_y1: sql<number>`f.amount_port_1mo_y1`.as('amount_port_1mo_y1'),
                    amount_port_2mo_y1: sql<number>`f.amount_port_2mo_y1`.as('amount_port_2mo_y1'),
                    amount_port_3mo_y1: sql<number>`f.amount_port_3mo_y1`.as('amount_port_3mo_y1'),
                    amount_port_4mo_y1: sql<number>`f.amount_port_4mo_y1`.as('amount_port_4mo_y1'),
                    amount_port_gt_6mo_y1: sql<number>`f.amount_port_gt_6mo_y1`.as('amount_port_gt_6mo_y1'),
                    amount_port_all_mo_y1: sql<number>`f.amount_port_all_mo_y1`.as('amount_port_all_mo_y1'),

                    occ_1mo_y: sql<string>`f.occ_1mo_y`.as('occ_1mo_y'),
                    occ_2mo_y: sql<string>`f.occ_2mo_y`.as('occ_2mo_y'),
                    occ_3mo_y: sql<string>`f.occ_3mo_y`.as('occ_3mo_y'),
                    occ_4mo_y: sql<string>`f.occ_4mo_y`.as('occ_4mo_y'),
                    occ_gt_6mo_y: sql<string>`f.occ_gt_6mo_y`.as('occ_gt_6mo_y'),
                    occ_all_mo_y: sql<string>`f.occ_all_mo_y`.as('occ_all_mo_y'),

                    occ_1mo_y1: sql<string>`f.occ_1mo_y1`.as('occ_1mo_y1'),
                    occ_2mo_y1: sql<string>`f.occ_2mo_y1`.as('occ_2mo_y1'),
                    occ_3mo_y1: sql<string>`f.occ_3mo_y1`.as('occ_3mo_y1'),
                    occ_4mo_y1: sql<string>`f.occ_4mo_y1`.as('occ_4mo_y1'),
                    occ_gt_6mo_y1: sql<string>`f.occ_gt_6mo_y1`.as('occ_gt_6mo_y1'),
                    occ_all_mo_y1: sql<string>`f.occ_all_mo_y1`.as('occ_all_mo_y1'),

                    occ_1mo_2y: sql<string>`CONCAT(ROUND((f.used_1mo_y + f.used_1mo_y1) / (f.amount_port_1mo_y + f.amount_port_1mo_y1) * 100 ,2), '%')`.as('occ_1mo_2y'),
                    occ_2mo_2y: sql<string>`CONCAT(ROUND((f.used_2mo_y + f.used_2mo_y1) / (f.amount_port_2mo_y + f.amount_port_2mo_y1) * 100 ,2), '%')`.as('occ_2mo_2y'),
                    occ_3mo_2y: sql<string>`CONCAT(ROUND((f.used_3mo_y + f.used_3mo_y1) / (f.amount_port_3mo_y + f.amount_port_3mo_y1) * 100 ,2), '%')`.as('occ_3mo_2y'),
                    occ_4mo_2y: sql<string>`CONCAT(ROUND((f.used_4mo_y + f.used_4mo_y1) / (f.amount_port_4mo_y + f.amount_port_4mo_y1) * 100 ,2), '%')`.as('occ_4mo_2y'),
                    occ_gt_6mo_2y: sql<string>`CONCAT(ROUND((f.used_gt_6mo_y + f.used_gt_6mo_y1) / (f.amount_port_gt_6mo_y + f.amount_port_gt_6mo_y1) * 100 ,2), '%')`.as('occ_gt_6mo_2y'),
                    occ_all_mo_2y: sql<string>`CONCAT(ROUND((f.used_all_mo_y + f.used_all_mo_y1) / (f.amount_port_all_mo_y + f.amount_port_all_mo_y1) * 100 ,2), '%')`.as('occ_all_mo_2y'),
                })
                .from(wokTerritory)
                .leftJoin(wokCurrOdp, sql`${wokTerritory.wok} = b.wok`)
                .leftJoin(wokPrevOdp, sql`${wokTerritory.wok} = c.wok`)
                .leftJoin(wokGoLive, sql`${wokTerritory.wok} = d.wok`)
                .leftJoin(wokDemands, sql`${wokTerritory.wok} = e.wok`)
                .leftJoin(wokGoLiveCurrYear, sql`${wokTerritory.wok} = f.wok`)
                .leftJoin(wokTargetDemands, sql`${wokTerritory.wok} = t.wok`)
                .groupBy(sql`1,2`)

            if (branch && wok) {
                const [finalDataRevenue] = await Promise.all([
                    wokFinalQuery
                ])

                return c.json({ data: finalDataRevenue })
            }

            if (branch) {
                const [finalDataRevenue] = await Promise.all([
                    branchFinalQuery
                ])

                return c.json({ data: finalDataRevenue })
            }

            const [finalDataRevenue] = await Promise.all([
                regionalFinalQuery
            ])

            return c.json({ data: finalDataRevenue })
        })
    .get('/demands-deployment-v2', zValidator('query', z.object({ date: z.string().optional(), branch: z.string().optional(), wok: z.string().optional() })),
        async c => {
            const { date, branch, wok } = c.req.valid('query')
            const selectedDate = date ? parseISO(date) : subDays(new Date(), 2)

            const currMonth = format(selectedDate, 'MM')
            const currYear = format(selectedDate, 'yyyy')
            const latestMonth = parseInt(format(selectedDate, 'M'), 10)
            const isPrevMonthLastYear = currMonth === '01'
            const prevMonth = isPrevMonthLastYear ? '12' : format(subMonths(selectedDate, 1), 'MM')
            const prevMonthYear = isPrevMonthLastYear ? format(subYears(selectedDate, 1), 'yyyy') : format(selectedDate, 'yyyy')
            const prevYear = format(subYears(selectedDate, 1), 'yyyy')

            const lastDayOfSelectedMonth = endOfMonth(selectedDate);
            const isEndOfMonth = selectedDate.getDate() === lastDayOfSelectedMonth.getDate();

            const endOfCurrMonth = isEndOfMonth ? lastDayOfSelectedMonth : selectedDate;
            const endOfPrevMonth = endOfMonth(subMonths(selectedDate, 1))
            const endOfPrevMonth2 = isEndOfMonth ? endOfMonth(subMonths(selectedDate, 1)) : subMonths(selectedDate, 1)
            const endOfPrevYearSameMonth = isEndOfMonth ? endOfMonth(subYears(selectedDate, 1)) : subYears(selectedDate, 1);

            const currStartOfMonth = format(startOfMonth(endOfCurrMonth), 'yyyy-MM-dd')
            const currDate = format(endOfCurrMonth, 'yyyy-MM-dd')
            const prevStartOfMonth = format(startOfMonth(endOfPrevMonth2), 'yyyy-MM-dd')
            const prevMonthSameDate = format(endOfPrevMonth2, 'yyyy-MM-dd')
            const prevDate = format(endOfPrevMonth, 'yyyy-MM-dd')
            const prevDate2 = format(endOfPrevMonth2, 'yyyy-MM-dd')
            const prevYearCurrDate = format(endOfPrevYearSameMonth, 'yyyy-MM-dd')

            const regionalTerritory = db
                .select({ regional: territoryHousehold.regional })
                .from(territoryHousehold)
                .where(eq(territoryHousehold.regional, 'MALUKU DAN PAPUA'))
                .groupBy(territoryHousehold.regional)
                .as('a')

            const regionalOdp = db
                .select()
                .from(summaryOdpRegional)
                .where(and(
                    eq(summaryOdpRegional.regional, 'MALUKU DAN PAPUA'),
                    eq(summaryOdpRegional.tgl, currDate)
                ))
                .as('b')

            const regionalGolive = db
                .select()
                .from(summaryGoliveRegional)
                .as('c')

            const regionalDemands = db
                .select({
                    regional: ih_demand_mysiis.region,
                    demand_created_mtd: sql<number>`SUM(CASE WHEN DATE_FORMAT(approved_at, '%Y-%m-%d') BETWEEN ${currStartOfMonth} AND ${currDate} THEN ${ih_demand_mysiis.target} END)`.as('demand_created_mtd'),
                    demand_created_m1: sql<number>`SUM(CASE WHEN DATE_FORMAT(approved_at, '%Y-%m-%d') BETWEEN ${prevStartOfMonth} AND ${prevMonthSameDate} THEN ${ih_demand_mysiis.target} END)`.as('demand_created_m1'),
                })
                .from(ih_demand_mysiis)
                .where(and(
                    gt(ih_demand_mysiis.target, 0),
                    eq(ih_demand_mysiis.region, 'MALUKU DAN PAPUA'),
                    inArray(ih_demand_mysiis.status_new_bispro, ['Wait Approval AI', 'Draft']),
                    sql`YEAR(${ih_demand_mysiis.approved_at}) = ${currYear}`
                ))
                .groupBy(sql`1`)
                .as('d')

            const regionalGolivePort = db
                .select()
                .from(summaryGolivePortRegional)
                .as('e')

            const regionalTargetDemands = db
                .select({
                    regional: sql<string>`b.regional`.as('regional'),
                    ytd_demands: sum(targetTerritoryDemands.ytd_demand).as('ytd_demands')
                })
                .from(targetTerritoryDemands)
                .rightJoin(
                    db
                        .selectDistinct({ regional: territoryHousehold.regional, branch: territoryHousehold.branch, wok: territoryHousehold.wok })
                        .from(territoryHousehold)
                        .as('b'),
                    and(
                        eq(targetTerritoryDemands.branch, sql`b.branch`),
                        eq(targetTerritoryDemands.wok, sql`b.wok`)
                    )
                )
                .where(eq(targetTerritoryDemands.periode, currYear))
                .groupBy(sql`1`)
                .as('t')

            const regionalFinalQuery = db
                .select({
                    name: regionalTerritory.regional,

                    used_black: regionalOdp.black_used_m,
                    used_red: regionalOdp.red_used_m,
                    used_green: regionalOdp.green_used_m,
                    used_yellow: regionalOdp.yellow_used_m,
                    used: regionalOdp.total_used_m,

                    used_black_m1: regionalOdp.black_used_m1,
                    used_red_m1: regionalOdp.red_used_m1,
                    used_green_m1: regionalOdp.green_used_m1,
                    used_yellow_m1: regionalOdp.yellow_used_m1,
                    used_m1: regionalOdp.total_used_m1,

                    avai_port_black: regionalOdp.black_avai_m,
                    avai_port_red: regionalOdp.red_avai_m,
                    avai_port_yellow: regionalOdp.yellow_avai_m,
                    avai_port_green: regionalOdp.green_avai_m,
                    avai_port: regionalOdp.total_avai_m,

                    avai_port_black_m1: regionalOdp.black_avai_m1,
                    avai_port_red_m1: regionalOdp.red_avai_m1,
                    avai_port_yellow_m1: regionalOdp.yellow_avai_m1,
                    avai_port_green_m1: regionalOdp.green_avai_m1,
                    avai_port_m1: regionalOdp.total_avai_m1,

                    amount_port_black: regionalOdp.black_amount_m,
                    amount_port_red: regionalOdp.red_amount_m,
                    amount_port_yellow: regionalOdp.yellow_amount_m,
                    amount_port_green: regionalOdp.green_amount_m,

                    amount_port_black_m1: regionalOdp.black_amount_m1,
                    amount_port_red_m1: regionalOdp.red_amount_m1,
                    amount_port_yellow_m1: regionalOdp.yellow_amount_m1,
                    amount_port_green_m1: regionalOdp.green_amount_m1,

                    odp_black: regionalOdp.black_odp_m,
                    odp_red: regionalOdp.red_odp_m,
                    odp_yellow: regionalOdp.yellow_odp_m,
                    odp_green: regionalOdp.green_odp_m,
                    odp: regionalOdp.total_odp_m,

                    golive_m: regionalGolive.golive_m,
                    golive_m1: regionalGolive.golive_m1,
                    golive_mom: sql<string>`CONCAT(c.golive_mom, '%')`.as('golive_mom'),
                    golive_m12: regionalGolive.golive_m12,
                    golive_y: regionalGolive.golive_y,
                    golive_y1: regionalGolive.golive_y1,
                    golive_yoy: sql<string>`CONCAT(c.golive_yoy, '%')`.as('golive_yoy'),
                    golive_ytd: sql<string>`CONCAT(c.golive_ytd, '%')`.as('golive_ytd'),

                    target_ytd_demand: regionalTargetDemands.ytd_demands,
                    demand_created_mtd: regionalDemands.demand_created_mtd,
                    demand_created_m1: regionalDemands.demand_created_m1,
                    demand_created_mom: sql<string>`CONCAT(ROUND((d.demand_created_mtd / d.demand_created_m1 - 1) * 100, 2), '%')`.as('demand_created_mom'),
                    ach_demands: sql<string>`CONCAT(COALESCE(ROUND(d.demand_created_mtd / t.ytd_demands * 100, 2) ,0), '%')`.as('ach_demands'),

                    avai_port_1mo_y: regionalGolivePort.avai_port_1mo_y,
                    avai_port_2mo_y: regionalGolivePort.avai_port_2mo_y,
                    avai_port_3mo_y: regionalGolivePort.avai_port_3mo_y,
                    avai_port_4mo_y: regionalGolivePort.avai_port_4mo_y,

                    used_1mo_y: regionalGolivePort.used_1mo_y,
                    used_2mo_y: regionalGolivePort.used_2mo_y,
                    used_3mo_y: regionalGolivePort.used_3mo_y,
                    used_4mo_y: regionalGolivePort.used_4mo_y,
                    used_gt_6mo_y: regionalGolivePort.used_gt_6mo_y,
                    used_all_mo_y: regionalGolivePort.used_all_mo_y,

                    amount_port_1mo_y: regionalGolivePort.amount_port_1mo_y,
                    amount_port_2mo_y: regionalGolivePort.amount_port_2mo_y,
                    amount_port_3mo_y: regionalGolivePort.amount_port_3mo_y,
                    amount_port_4mo_y: regionalGolivePort.amount_port_4mo_y,
                    amount_port_gt_6mo_y: regionalGolivePort.amount_port_gt_6mo_y,
                    amount_port_all_mo_y: regionalGolivePort.amount_port_all_mo_y,

                    used_1mo_y1: regionalGolivePort.used_1mo_y1,
                    used_2mo_y1: regionalGolivePort.used_2mo_y1,
                    used_3mo_y1: regionalGolivePort.used_3mo_y1,
                    used_4mo_y1: regionalGolivePort.used_4mo_y1,
                    used_gt_6mo_y1: regionalGolivePort.used_gt_6mo_y1,
                    used_all_mo_y1: regionalGolivePort.used_all_mo_y1,

                    amount_port_1mo_y1: regionalGolivePort.amount_port_1mo_y1,
                    amount_port_2mo_y1: regionalGolivePort.amount_port_2mo_y1,
                    amount_port_3mo_y1: regionalGolivePort.amount_port_3mo_y1,
                    amount_port_4mo_y1: regionalGolivePort.amount_port_4mo_y1,
                    amount_port_gt_6mo_y1: regionalGolivePort.amount_port_gt_6mo_y1,
                    amount_port_all_mo_y1: regionalGolivePort.amount_port_all_mo_y1,

                    occ_1mo_y: sql<string>`CONCAT(COALESCE(ROUND((e.used_1mo_y / e.amount_port_1mo_y - 1) * 100, 2), 0), '%')`.as('occ_1mo_y'),
                    occ_2mo_y: sql<string>`CONCAT(COALESCE(ROUND((e.used_2mo_y / e.amount_port_2mo_y - 1) * 100, 2), 0), '%')`.as('occ_2mo_y'),
                    occ_3mo_y: sql<string>`CONCAT(COALESCE(ROUND((e.used_3mo_y / e.amount_port_3mo_y - 1) * 100, 2), 0), '%')`.as('occ_3mo_y'),
                    occ_4mo_y: sql<string>`CONCAT(COALESCE(ROUND((e.used_4mo_y / e.amount_port_4mo_y - 1) * 100, 2), 0), '%')`.as('occ_4mo_y'),
                    occ_gt_6mo_y: sql<string>`CONCAT(COALESCE(ROUND((e.used_gt_6mo_y / e.amount_port_gt_6mo_y - 1) * 100, 2), 0), '%')`.as('occ_gt_6mo_y'),
                    occ_all_mo_y: sql<string>`CONCAT(COALESCE(ROUND((e.used_all_mo_y / e.amount_port_all_mo_y - 1) * 100, 2), 0), '%')`.as('occ_all_mo_y'),

                    occ_1mo_y1: sql<string>`CONCAT(COALESCE(ROUND((e.used_1mo_y1 / e.amount_port_1mo_y1 - 1) * 100, 2), 0), '%')`.as('occ_1mo_y'),
                    occ_2mo_y1: sql<string>`CONCAT(COALESCE(ROUND((e.used_2mo_y1 / e.amount_port_2mo_y1 - 1) * 100, 2), 0), '%')`.as('occ_2mo_y'),
                    occ_3mo_y1: sql<string>`CONCAT(COALESCE(ROUND((e.used_3mo_y1 / e.amount_port_3mo_y1 - 1) * 100, 2), 0), '%')`.as('occ_3mo_y'),
                    occ_4mo_y1: sql<string>`CONCAT(COALESCE(ROUND((e.used_4mo_y1 / e.amount_port_4mo_y1 - 1) * 100, 2), 0), '%')`.as('occ_4mo_y'),
                    occ_gt_6mo_y1: sql<string>`CONCAT(COALESCE(ROUND((e.used_gt_6mo_y1 / e.amount_port_gt_6mo_y1 - 1) * 100, 2), 0), '%')`.as('occ_gt_6mo_y'),
                    occ_all_mo_y1: sql<string>`CONCAT(COALESCE(ROUND((e.used_all_mo_y1 / e.amount_port_all_mo_y1 - 1) * 100, 2), 0), '%')`.as('occ_all_mo_y'),
                })
                .from(regionalTerritory)
                .leftJoin(regionalOdp, eq(regionalTerritory.regional, regionalOdp.regional))
                .leftJoin(regionalGolive, eq(regionalTerritory.regional, regionalGolive.regional))
                .leftJoin(regionalDemands, eq(regionalTerritory.regional, regionalDemands.regional))
                .leftJoin(regionalGolivePort, eq(regionalTerritory.regional, regionalGolivePort.regional))
                .leftJoin(regionalTargetDemands, eq(regionalTerritory.regional, regionalTargetDemands.regional))
                .groupBy(regionalTerritory.regional)

            const [finalDataRevenue] = await Promise.all([
                regionalFinalQuery
            ])

            return c.json({ data: finalDataRevenue })
        })
    .get('/sf-class', zValidator('query', z.object({ date: z.string().optional(), branch: z.string().optional(), wok: z.string().optional() })),
        async c => {
            const { date, branch, wok } = c.req.valid('query')
            const selectedDate = date ? parseISO(date) : subDays(new Date(), 2)

            const currMonth = format(selectedDate, 'MM')
            const currYear = format(selectedDate, 'yyyy')
            const lastDayOfSelectedMonth = endOfMonth(selectedDate);
            const isEndOfMonth = selectedDate.getDate() === lastDayOfSelectedMonth.getDate();

            const ihOrderingDetailOrder = dynamicIhOrderingDetailOrderTable(currYear, currMonth)

            const currStartOfMonth = format(startOfMonth(selectedDate), 'yyyy-MM-dd')
            const currDate = format(selectedDate, 'yyyy-MM-dd')

            const regionalSFData = db
                .select({
                    sf_code: ihOrderingDetailOrder.sf_code,
                    regional: sql<string>`region`.as('regional'),
                    ps: count(ihOrderingDetailOrder.ps_ts).as('ps')
                })
                .from(ihOrderingDetailOrder)
                .where(and(
                    eq(ihOrderingDetailOrder.region, 'MALUKU DAN PAPUA'),
                    eq(ihOrderingDetailOrder.order_type, 'NEW SALES'),
                    eq(ihOrderingDetailOrder.order_channel, 'i4'),
                    inArray(ihOrderingDetailOrder.funneling_group, ['COMPLETED', 'PS']),
                    isNotNull(ihOrderingDetailOrder.ps_ts),
                    isNotNull(ihOrderingDetailOrder.sf_code),
                    sql`DATE_FORMAT(${ihOrderingDetailOrder.ps_ts}, '%Y-%m-%d') BETWEEN ${currStartOfMonth} AND ${currDate}`,
                ))
                .groupBy(ihOrderingDetailOrder.sf_code)
                .as('a')

            const regionalSFClassification = db
                .select({
                    sf_code: regionalSFData.sf_code,
                    regional: regionalSFData.regional,
                    ps: regionalSFData.ps,
                    category: sql<string>`CASE
                        WHEN a.ps BETWEEN 0 AND 1 THEN 'BLACK'
                        WHEN a.ps BETWEEN 2 AND 5 THEN 'BRONZE'
                        WHEN a.ps BETWEEN 6 AND 10 THEN 'SILVER'
                        WHEN a.ps BETWEEN 11 AND 20 THEN 'GOLD'
                        WHEN a.ps > 20 THEN 'PLATINUM'
                    END`.as('category')
                })
                .from(regionalSFData)
                .groupBy(sql`1`)
                .as('b')

            const regionalSFSummary = db
                .select({
                    name: regionalSFClassification.regional,
                    sf_black: sql<number>`COUNT(CASE WHEN category = 'BLACK' THEN sf_code END)`.as('sf_black'),
                    sf_bronze: sql<number>`COUNT(CASE WHEN category = 'BRONZE' THEN sf_code END)`.as('sf_bronze'),
                    sf_silver: sql<number>`COUNT(CASE WHEN category = 'SILVER' THEN sf_code END)`.as('sf_silver'),
                    sf_gold: sql<number>`COUNT(CASE WHEN category = 'GOLD' THEN sf_code END)`.as('sf_gold'),
                    sf_platinum: sql<number>`COUNT(CASE WHEN category = 'PLATINUM' THEN sf_code END)`.as('sf_platinum'),
                    total_sf: sql<number>`COUNT(sf_code)`.as('total_sf')
                })
                .from(regionalSFClassification)
                .groupBy(sql`1`)

            const branchSFData = db
                .select({
                    sf_code: ihOrderingDetailOrder.sf_code,
                    branch: ihOrderingDetailOrder.branch,
                    ps: count(ihOrderingDetailOrder.ps_ts).as('ps')
                })
                .from(ihOrderingDetailOrder)
                .where(and(
                    eq(ihOrderingDetailOrder.region, 'MALUKU DAN PAPUA'),
                    eq(ihOrderingDetailOrder.order_type, 'NEW SALES'),
                    eq(ihOrderingDetailOrder.order_channel, 'i4'),
                    inArray(ihOrderingDetailOrder.funneling_group, ['COMPLETED', 'PS']),
                    isNotNull(ihOrderingDetailOrder.ps_ts),
                    isNotNull(ihOrderingDetailOrder.sf_code),
                    sql`DATE_FORMAT(${ihOrderingDetailOrder.ps_ts}, '%Y-%m-%d') BETWEEN ${currStartOfMonth} AND ${currDate}`,
                    branch && branch !== '' ? eq(ihOrderingDetailOrder.branch, branch) : undefined
                ))
                .groupBy(ihOrderingDetailOrder.sf_code)
                .as('a')

            const branchSFClassification = db
                .select({
                    sf_code: branchSFData.sf_code,
                    branch: branchSFData.branch,
                    ps: branchSFData.ps,
                    category: sql<string>`CASE
                        WHEN a.ps BETWEEN 0 AND 1 THEN 'BLACK'
                        WHEN a.ps BETWEEN 2 AND 5 THEN 'BRONZE'
                        WHEN a.ps BETWEEN 6 AND 10 THEN 'SILVER'
                        WHEN a.ps BETWEEN 11 AND 20 THEN 'GOLD'
                        WHEN a.ps > 20 THEN 'PLATINUM'
                    END`.as('category')
                })
                .from(branchSFData)
                .groupBy(sql`1`)
                .as('b')

            const branchSFSummary = db
                .select({
                    name: branchSFClassification.branch,
                    sf_black: sql<number>`COUNT(CASE WHEN category = 'BLACK' THEN sf_code END)`.as('sf_black'),
                    sf_bronze: sql<number>`COUNT(CASE WHEN category = 'BRONZE' THEN sf_code END)`.as('sf_bronze'),
                    sf_silver: sql<number>`COUNT(CASE WHEN category = 'SILVER' THEN sf_code END)`.as('sf_silver'),
                    sf_gold: sql<number>`COUNT(CASE WHEN category = 'GOLD' THEN sf_code END)`.as('sf_gold'),
                    sf_platinum: sql<number>`COUNT(CASE WHEN category = 'PLATINUM' THEN sf_code END)`.as('sf_platinum'),
                    total_sf: sql<number>`COUNT(sf_code)`.as('total_sf')
                })
                .from(branchSFClassification)
                .groupBy(sql`1`)

            const wokSFData = db
                .select({
                    sf_code: ihOrderingDetailOrder.sf_code,
                    wok: ihOrderingDetailOrder.wok,
                    ps: count(ihOrderingDetailOrder.ps_ts).as('ps')
                })
                .from(ihOrderingDetailOrder)
                .where(and(
                    eq(ihOrderingDetailOrder.region, 'MALUKU DAN PAPUA'),
                    eq(ihOrderingDetailOrder.order_type, 'NEW SALES'),
                    eq(ihOrderingDetailOrder.order_channel, 'i4'),
                    inArray(ihOrderingDetailOrder.funneling_group, ['COMPLETED', 'PS']),
                    isNotNull(ihOrderingDetailOrder.ps_ts),
                    isNotNull(ihOrderingDetailOrder.sf_code),
                    sql`DATE_FORMAT(${ihOrderingDetailOrder.ps_ts}, '%Y-%m-%d') BETWEEN ${currStartOfMonth} AND ${currDate}`,
                    wok ? eq(ihOrderingDetailOrder.wok, wok) : undefined
                ))
                .groupBy(ihOrderingDetailOrder.sf_code)
                .as('a')

            const wokSFClassification = db
                .select({
                    sf_code: wokSFData.sf_code,
                    wok: wokSFData.wok,
                    ps: wokSFData.ps,
                    category: sql<string>`CASE
                        WHEN a.ps BETWEEN 0 AND 1 THEN 'BLACK'
                        WHEN a.ps BETWEEN 2 AND 5 THEN 'BRONZE'
                        WHEN a.ps BETWEEN 6 AND 10 THEN 'SILVER'
                        WHEN a.ps BETWEEN 11 AND 20 THEN 'GOLD'
                        WHEN a.ps > 20 THEN 'PLATINUM'
                    END`.as('category')
                })
                .from(wokSFData)
                .groupBy(sql`1`)
                .as('b')

            const wokSFSummary = db
                .select({
                    name: wokSFClassification.wok,
                    sf_black: sql<number>`COUNT(CASE WHEN category = 'BLACK' THEN sf_code END)`.as('sf_black'),
                    sf_bronze: sql<number>`COUNT(CASE WHEN category = 'BRONZE' THEN sf_code END)`.as('sf_bronze'),
                    sf_silver: sql<number>`COUNT(CASE WHEN category = 'SILVER' THEN sf_code END)`.as('sf_silver'),
                    sf_gold: sql<number>`COUNT(CASE WHEN category = 'GOLD' THEN sf_code END)`.as('sf_gold'),
                    sf_platinum: sql<number>`COUNT(CASE WHEN category = 'PLATINUM' THEN sf_code END)`.as('sf_platinum'),
                    total_sf: sql<number>`COUNT(sf_code)`.as('total_sf')
                })
                .from(wokSFClassification)
                .groupBy(sql`1`)

            if (branch && wok) {
                const [finalDataRevenue] = await Promise.all([
                    wokSFSummary
                ])

                return c.json({ data: finalDataRevenue })
            }

            if (branch) {
                const [finalDataRevenue] = await Promise.all([
                    branchSFSummary
                ])

                return c.json({ data: finalDataRevenue })
            }

            const [finalDataRevenue] = await Promise.all([
                regionalSFSummary
            ])

            return c.json({ data: finalDataRevenue })
        })
    .get('/revenue-c3mr', zValidator('query', z.object({ date: z.string().optional(), branch: z.string().optional(), wok: z.string().optional() })),
        async c => {
            const { date, branch, wok } = c.req.valid('query')
            const selectedDate = date ? parseISO(date) : subDays(new Date(), 2)

            const currMonth = format(selectedDate, 'MM')
            const currYear = format(selectedDate, 'yyyy')
            const lastDayOfSelectedMonth = endOfMonth(selectedDate);
            const isEndOfMonth = selectedDate.getDate() === lastDayOfSelectedMonth.getDate();

            const ihC3mr = dynamicIhC3mr(currYear, currMonth)

            const currStartOfMonth = format(startOfMonth(selectedDate), 'yyyy-MM-dd')
            const currDate = format(selectedDate, 'yyyy-MM-dd')
            const yyyyMM = format(selectedDate, 'yyyyMM')

            const regionalTerritory = db
                .select({
                    regional: territoryHousehold.regional
                })
                .from(territoryHousehold)
                .where(eq(territoryHousehold.regional, 'MALUKU DAN PAPUA'))
                .groupBy(sql`1`)
                .as('a')

            const summaryRegional = db
                .select({
                    regional: ihC3mr.region,
                    bill_amount_all: sql<number>`ROUND(SUM(bill_amount) / 1000000000, 2)`.as('bill_amount_all'),
                    bill_amount_ns: sql<number>`ROUND(SUM(CASE WHEN billing_category = 'PSB' THEN bill_amount END) / 1000000000, 2)`.as('bill_amount_ns'),
                    bill_amount_existing: sql<number>`ROUND(SUM(CASE WHEN billing_category = 'EXISTING' THEN bill_amount END) / 1000000000, 2)`.as('bill_amount_existing'),

                    subs_0_6: sql<number>`COUNT(CASE WHEN los_category IN ('0-3 bulan', '4-6 bulan') THEN 1 END)`.as('subs_0_6'),
                    subs_paid_0_6: sql<number>`COUNT(CASE WHEN paid_flag = 1 and los_category IN ('0-3 bulan', '4-6 bulan') THEN 1 END)`.as('subs_paid_0_6'),
                    subs_unpaid_0_6: sql<number>`COUNT(CASE WHEN paid_flag = 0 and los_category IN ('0-3 bulan', '4-6 bulan') THEN 1 END)`.as('subs_unpaid_0_6'),
                    subs_gt_6: sql<number>`COUNT(CASE WHEN los_category IN ('7-12 bulan', '12-24 bulan','>24 bulan') THEN 1 END)`.as('subs_gt_6'),
                    subs_paid_gt_6: sql<number>`COUNT(CASE WHEN paid_flag = 1 and los_category IN ('7-12 bulan', '12-24 bulan','>24 bulan') THEN 1 END)`.as('subs_paid_gt_6'),
                    subs_unpaid_gt_6: sql<number>`COUNT(CASE WHEN paid_flag = 0 and los_category IN ('7-12 bulan', '12-24 bulan','>24 bulan') THEN 1 END)`.as('subs_unpaid_gt_6'),

                    bill_amount_all_unpaid: sql<number>`ROUND(SUM(CASE WHEN paid_flag = 0 THEN bill_amount END) / 1000000000, 2)`.as('bill_amount_all_unpaid'),
                    bill_amount_ns_unpaid: sql<number>`ROUND(SUM(CASE WHEN billing_category = 'PSB' AND paid_flag = 0 THEN bill_amount END) / 1000000000, 2)`.as('bill_amount_ns_unpaid'),
                    bill_amount_existing_unpaid: sql<number>`ROUND(SUM(CASE WHEN billing_category = 'EXISTING' AND paid_flag = 0 THEN bill_amount END) / 1000000000, 2)`.as('bill_amount_existing_unpaid')
                })
                .from(ihC3mr)
                .groupBy(sql`1`)
                .as('b')

            const regionalTargetRevenue = db
                .select({
                    regional: sql<string>`CASE WHEN b.branch IN ('TIMIKA', 'SORONG', 'AMBON', 'JAYAPURA') THEN 'MALUKU DAN PAPUA' END`.as('regional'),
                    target_rev_all: sql<number>`ROUND(SUM(rev_all), 2)`.as('target_rev_all'),
                    target_rev_ns: sql<number>`ROUND(SUM(rev_ns), 2)`.as('target_rev_ns'),
                    target_rev_existing: sql<number>`ROUND(SUM(rev_existing), 2)`.as('target_rev_existing'),
                })
                .from(targetRevenueC3mr)
                .rightJoin(
                    db.selectDistinct({ regional: territoryHousehold.regional, branch: territoryHousehold.branch, wok: territoryHousehold.wok }).from(territoryHousehold).as('b'),
                    and(eq(targetRevenueC3mr.branch, sql`b.branch`), eq(targetRevenueC3mr.wok, sql`b.wok`))
                )
                .where(eq(targetRevenueC3mr.periode, yyyyMM))
                .groupBy(sql`1`)
                .as('c')

            const regionalFinalQuery = db
                .select({
                    name: sql<string>`a.regional`.as('regional'),
                    target_rev_all: sql<number>`ROUND(SUM(c.target_rev_all), 2)`.as('target_rev_all'),
                    bill_amount_all: sql<number>`ROUND(SUM(b.bill_amount_all), 2)`.as('bill_amount_all'),
                    bill_amount_all_unpaid: sql<number>`ROUND(SUM(b.bill_amount_all_unpaid), 2)`.as('bill_amount_all_unpaid'),
                    ach_fm_rev_all: sql<string>`CONCAT(ROUND(SUM(b.bill_amount_all) / SUM(c.target_rev_all) * 100, 2), '%')`.as('ach_fm_rev_all'),
                    gap_to_target_rev_all: sql<number>`ROUND((SUM(b.bill_amount_all)) - SUM(c.target_rev_all), 2)`.as('gap_to_target_rev_all'),

                    target_rev_ns: sql<number>`ROUND(SUM(c.target_rev_ns), 2)`.as('target_rev_ns'),
                    bill_amount_ns: sql<number>`ROUND(SUM(b.bill_amount_ns), 2)`.as('bill_amount_ns'),
                    bill_amount_ns_unpaid: sql<number>`ROUND(SUM(b.bill_amount_ns_unpaid), 2)`.as('bill_amount_ns_unpaid'),
                    ach_fm_rev_ns: sql<string>`CONCAT(ROUND(SUM(b.bill_amount_ns) / SUM(c.target_rev_ns) * 100, 2), '%')`.as('ach_fm_rev_ns'),
                    gap_to_target_rev_ns: sql<number>`ROUND((SUM(b.bill_amount_ns)) - SUM(c.target_rev_ns), 2)`.as('gap_to_target_rev_ns'),

                    target_rev_existing: sql<number>`ROUND(SUM(c.target_rev_existing), 2)`.as('target_rev_existing'),
                    bill_amount_existing: sql<number>`ROUND(SUM(b.bill_amount_existing), 2)`.as('bill_amount_existing'),
                    bill_amount_existing_unpaid: sql<number>`ROUND(SUM(b.bill_amount_existing_unpaid), 2)`.as('bill_amount_existing_unpaid'),
                    ach_fm_rev_existing: sql<string>`CONCAT(ROUND(SUM(b.bill_amount_existing) / SUM(c.target_rev_existing) * 100, 2), '%')`.as('ach_fm_rev_existing'),
                    gap_to_target_rev_existing: sql<number>`ROUND(SUM(b.bill_amount_existing)  - SUM(c.target_rev_existing), 2)`.as('gap_to_target_rev_existing'),

                    subs_0_6: summaryRegional.subs_0_6,
                    subs_paid_0_6: summaryRegional.subs_paid_0_6,
                    ach_subs_0_6: sql<string>`CONCAT(ROUND(b.subs_paid_0_6 / b.subs_0_6 * 100, 2), '%')`.as('ach_subs_0_6'),
                    subs_gt_6: summaryRegional.subs_gt_6,
                    subs_paid_gt_6: summaryRegional.subs_paid_gt_6,
                    ach_subs_paid_gt_6: sql<string>`CONCAT(ROUND(b.subs_paid_gt_6 / b.subs_gt_6 * 100, 2), '%')`.as('ach_subs_paid_gt_6'),

                    revenue_loss: sql<number>`ROUND((SUM(b.bill_amount_ns_unpaid) - SUM(b.bill_amount_all_unpaid)), 2)`.as('revenue_loss')
                })
                .from(regionalTerritory)
                .leftJoin(summaryRegional, eq(regionalTerritory.regional, summaryRegional.regional))
                .leftJoin(regionalTargetRevenue, eq(regionalTerritory.regional, sql`c.regional`))
                .groupBy(sql`1`)

            const branchTerritory = db
                .select({
                    branch: territoryHousehold.branch
                })
                .from(territoryHousehold)
                .where(branch && branch !== '' ? and(
                    eq(territoryHousehold.regional, 'MALUKU DAN PAPUA'),
                    eq(territoryHousehold.branch, branch)) :
                    eq(territoryHousehold.regional, 'MALUKU DAN PAPUA')
                )
                .groupBy(sql`1`)
                .as('a')

            const summaryBranch = db
                .select({
                    branch: ihC3mr.branch,
                    bill_amount_all: sql<number>`ROUND(SUM(bill_amount) / 1000000000, 2)`.as('bill_amount_all'),
                    bill_amount_ns: sql<number>`ROUND(SUM(CASE WHEN billing_category = 'PSB' THEN bill_amount END) / 1000000000, 2)`.as('bill_amount_ns'),
                    bill_amount_existing: sql<number>`ROUND(SUM(CASE WHEN billing_category = 'EXISTING' THEN bill_amount END) / 1000000000, 2)`.as('bill_amount_existing'),

                    subs_0_6: sql<number>`COUNT(CASE WHEN los_category IN ('0-3 bulan', '4-6 bulan') THEN 1 END)`.as('subs_0_6'),
                    subs_paid_0_6: sql<number>`COUNT(CASE WHEN paid_flag = 1 and los_category IN ('0-3 bulan', '4-6 bulan') THEN 1 END)`.as('subs_paid_0_6'),
                    subs_unpaid_0_6: sql<number>`COUNT(CASE WHEN paid_flag = 0 and los_category IN ('0-3 bulan', '4-6 bulan') THEN 1 END)`.as('subs_unpaid_0_6'),
                    subs_gt_6: sql<number>`COUNT(CASE WHEN los_category IN ('7-12 bulan', '12-24 bulan','>24 bulan') THEN 1 END)`.as('subs_gt_6'),
                    subs_paid_gt_6: sql<number>`COUNT(CASE WHEN paid_flag = 1 and los_category IN ('7-12 bulan', '12-24 bulan','>24 bulan') THEN 1 END)`.as('subs_paid_gt_6'),
                    subs_unpaid_gt_6: sql<number>`COUNT(CASE WHEN paid_flag = 0 and los_category IN ('7-12 bulan', '12-24 bulan','>24 bulan') THEN 1 END)`.as('subs_unpaid_gt_6'),

                    bill_amount_all_unpaid: sql<number>`ROUND(SUM(CASE WHEN paid_flag = 0 THEN bill_amount END) / 1000000000, 2)`.as('bill_amount_all_unpaid'),
                    bill_amount_ns_unpaid: sql<number>`ROUND(SUM(CASE WHEN billing_category = 'PSB' AND paid_flag = 0 THEN bill_amount END) / 1000000000, 2)`.as('bill_amount_ns_unpaid'),
                    bill_amount_existing_unpaid: sql<number>`ROUND(SUM(CASE WHEN billing_category = 'EXISTING' AND paid_flag = 0 THEN bill_amount END) / 1000000000, 2)`.as('bill_amount_existing_unpaid')
                })
                .from(ihC3mr)
                .groupBy(sql`1`)
                .as('b')

            const branchTargetRevenue = db
                .select({
                    branch: targetRevenueC3mr.branch,
                    target_rev_all: sql<number>`SUM(rev_all)`.as('target_rev_all'),
                    target_rev_ns: sql<number>`SUM(rev_ns)`.as('target_rev_ns'),
                    target_rev_existing: sql<number>`SUM(rev_existing)`.as('target_rev_existing'),
                })
                .from(targetRevenueC3mr)
                .rightJoin(
                    db.selectDistinct({ branch: territoryHousehold.branch, wok: territoryHousehold.wok }).from(territoryHousehold).as('b'),
                    and(eq(targetRevenueC3mr.branch, sql`b.branch`), eq(targetRevenueC3mr.wok, sql`b.wok`))
                )
                .where(eq(targetRevenueC3mr.periode, yyyyMM))
                .groupBy(sql`1`)
                .as('c')

            const branchFinalQuery = db
                .select({
                    name: sql<string>`a.branch`.as('branch'),
                    target_rev_all: sql<number>`ROUND(SUM(c.target_rev_all), 2)`.as('target_rev_all'),
                    bill_amount_all: sql<number>`ROUND(SUM(b.bill_amount_all), 2)`.as('bill_amount_all'),
                    bill_amount_all_unpaid: sql<number>`ROUND(SUM(b.bill_amount_all_unpaid), 2)`.as('bill_amount_all_unpaid'),
                    ach_fm_rev_all: sql<string>`CONCAT(ROUND(SUM(b.bill_amount_all) / SUM(c.target_rev_all) * 100, 2), '%')`.as('ach_fm_rev_all'),
                    gap_to_target_rev_all: sql<number>`ROUND((SUM(b.bill_amount_all)) - SUM(c.target_rev_all), 2)`.as('gap_to_target_rev_all'),

                    target_rev_ns: sql<number>`ROUND(SUM(c.target_rev_ns), 2)`.as('target_rev_ns'),
                    bill_amount_ns: sql<number>`ROUND(SUM(b.bill_amount_ns), 2)`.as('bill_amount_ns'),
                    bill_amount_ns_unpaid: sql<number>`ROUND(SUM(b.bill_amount_ns_unpaid), 2)`.as('bill_amount_ns_unpaid'),
                    ach_fm_rev_ns: sql<string>`CONCAT(ROUND(SUM(b.bill_amount_ns) / SUM(c.target_rev_ns) * 100, 2), '%')`.as('ach_fm_rev_ns'),
                    gap_to_target_rev_ns: sql<number>`ROUND((SUM(b.bill_amount_ns)) - SUM(c.target_rev_ns), 2)`.as('gap_to_target_rev_ns'),

                    target_rev_existing: sql<number>`ROUND(SUM(c.target_rev_existing), 2)`.as('target_rev_existing'),
                    bill_amount_existing: sql<number>`ROUND(SUM(b.bill_amount_existing), 2)`.as('bill_amount_existing'),
                    bill_amount_existing_unpaid: sql<number>`ROUND(SUM(b.bill_amount_existing_unpaid), 2)`.as('bill_amount_existing_unpaid'),
                    ach_fm_rev_existing: sql<string>`CONCAT(ROUND(SUM(b.bill_amount_existing) / SUM(c.target_rev_existing) * 100, 2), '%')`.as('ach_fm_rev_existing'),
                    gap_to_target_rev_existing: sql<number>`ROUND(SUM(b.bill_amount_existing)  - SUM(c.target_rev_existing), 2)`.as('gap_to_target_rev_existing'),

                    subs_0_6: summaryBranch.subs_0_6,
                    subs_paid_0_6: summaryBranch.subs_paid_0_6,
                    ach_subs_0_6: sql<string>`CONCAT(ROUND(b.subs_paid_0_6 / b.subs_0_6 * 100, 2), '%')`.as('ach_subs_0_6'),
                    subs_gt_6: summaryBranch.subs_gt_6,
                    subs_paid_gt_6: summaryBranch.subs_paid_gt_6,
                    ach_subs_paid_gt_6: sql<string>`CONCAT(ROUND(b.subs_paid_gt_6 / b.subs_gt_6 * 100, 2), '%')`.as('ach_subs_paid_gt_6'),

                    revenue_loss: sql<number>`ROUND((SUM(b.bill_amount_ns_unpaid) - SUM(b.bill_amount_all_unpaid)), 2)`.as('revenue_loss')
                })
                .from(branchTerritory)
                .leftJoin(summaryBranch, eq(branchTerritory.branch, summaryBranch.branch))
                .leftJoin(branchTargetRevenue, eq(branchTerritory.branch, branchTargetRevenue.branch))
                .groupBy(sql`1`)
                .orderBy(sql`1`)

            const wokTerritory = db
                .select({
                    wok: territoryHousehold.wok
                })
                .from(territoryHousehold)
                .where(branch && wok ? and(
                    eq(territoryHousehold.regional, 'MALUKU DAN PAPUA'),
                    eq(territoryHousehold.branch, branch),
                    eq(territoryHousehold.wok, wok),
                ) :
                    eq(territoryHousehold.regional, 'MALUKU DAN PAPUA')
                )
                .groupBy(sql`1`)
                .as('a')

            const summaryWok = db
                .select({
                    wok: sql<string>`
                    CASE
                        WHEN sto IN ('WHA','TUA','SPR','SML','NML','NIR','MSH','LRA','DOB','BUL') THEN 'AMBON OUTER'
                        WHEN sto IN ('PAO', 'ABO') THEN 'AMBON INNER'
                        WHEN sto IN ('WAM','SRU','SRM','SNI','BIA') THEN 'JAYAPURA OUTER'
                        WHEN sto IN ('WAE','JPB','JAP','ABE') THEN 'JAYAPURA INNER'
                        WHEN sto IN ('TIM','TBG','KUK','NAB') THEN 'MIMIKA'
                        WHEN sto IN ('TMR','MRK','BAD','AGT') THEN 'MERAUKE'
                        WHEN sto IN ('WMR','RSK','MWR','KIN','FFA','BTI') THEN 'MANOKWARI NABIRE'
                        WHEN sto IN ('TMB', 'SON') THEN 'SORONG RAJA AMPAT'
                    END`.as('wok'),
                    bill_amount_all: sql<number>`ROUND(SUM(bill_amount) / 1000000000, 2)`.as('bill_amount_all'),
                    bill_amount_ns: sql<number>`ROUND(SUM(CASE WHEN billing_category = 'PSB' THEN bill_amount END) / 1000000000, 2)`.as('bill_amount_ns'),
                    bill_amount_existing: sql<number>`ROUND(SUM(CASE WHEN billing_category = 'EXISTING' THEN bill_amount END) / 1000000000, 2)`.as('bill_amount_existing'),

                    subs_0_6: sql<number>`COUNT(CASE WHEN los_category IN ('0-3 bulan', '4-6 bulan') THEN 1 END)`.as('subs_0_6'),
                    subs_paid_0_6: sql<number>`COUNT(CASE WHEN paid_flag = 1 and los_category IN ('0-3 bulan', '4-6 bulan') THEN 1 END)`.as('subs_paid_0_6'),
                    subs_unpaid_0_6: sql<number>`COUNT(CASE WHEN paid_flag = 0 and los_category IN ('0-3 bulan', '4-6 bulan') THEN 1 END)`.as('subs_unpaid_0_6'),
                    subs_gt_6: sql<number>`COUNT(CASE WHEN los_category IN ('7-12 bulan', '12-24 bulan','>24 bulan') THEN 1 END)`.as('subs_gt_6'),
                    subs_paid_gt_6: sql<number>`COUNT(CASE WHEN paid_flag = 1 and los_category IN ('7-12 bulan', '12-24 bulan','>24 bulan') THEN 1 END)`.as('subs_paid_gt_6'),
                    subs_unpaid_gt_6: sql<number>`COUNT(CASE WHEN paid_flag = 0 and los_category IN ('7-12 bulan', '12-24 bulan','>24 bulan') THEN 1 END)`.as('subs_unpaid_gt_6'),

                    bill_amount_all_unpaid: sql<number>`ROUND(SUM(CASE WHEN paid_flag = 0 THEN bill_amount END) / 1000000000, 2)`.as('bill_amount_all_unpaid'),
                    bill_amount_ns_unpaid: sql<number>`ROUND(SUM(CASE WHEN billing_category = 'PSB' AND paid_flag = 0 THEN bill_amount END) / 1000000000, 2)`.as('bill_amount_ns_unpaid'),
                    bill_amount_existing_unpaid: sql<number>`ROUND(SUM(CASE WHEN billing_category = 'EXISTING' AND paid_flag = 0 THEN bill_amount END) / 1000000000, 2)`.as('bill_amount_existing_unpaid')
                })
                .from(ihC3mr)
                .groupBy(sql`1`)
                .as('b')

            const wokTargetRevenue = db
                .select({
                    wok: targetRevenueC3mr.wok,
                    target_rev_all: sql<number>`SUM(rev_all)`.as('target_rev_all'),
                    target_rev_ns: sql<number>`SUM(rev_ns)`.as('target_rev_ns'),
                    target_rev_existing: sql<number>`SUM(rev_existing)`.as('target_rev_existing'),
                })
                .from(targetRevenueC3mr)
                .rightJoin(
                    db.selectDistinct({ branch: territoryHousehold.branch, wok: territoryHousehold.wok }).from(territoryHousehold).as('b'),
                    and(eq(targetRevenueC3mr.branch, sql`b.branch`), eq(targetRevenueC3mr.wok, sql`b.wok`))
                )
                .where(eq(targetRevenueC3mr.periode, yyyyMM))
                .groupBy(sql`1`)
                .as('c')

            const wokFinalQuery = db
                .select({
                    name: sql<string>`a.wok`.as('wok'),
                    target_rev_all: sql<number>`ROUND(SUM(c.target_rev_all), 2)`.as('target_rev_all'),
                    bill_amount_all: sql<number>`ROUND(SUM(b.bill_amount_all), 2)`.as('bill_amount_all'),
                    bill_amount_all_unpaid: sql<number>`ROUND(SUM(b.bill_amount_all_unpaid), 2)`.as('bill_amount_all_unpaid'),
                    ach_fm_rev_all: sql<string>`CONCAT(ROUND(SUM(b.bill_amount_all) / SUM(c.target_rev_all) * 100, 2), '%')`.as('ach_fm_rev_all'),
                    gap_to_target_rev_all: sql<number>`ROUND((SUM(b.bill_amount_all)) - SUM(c.target_rev_all), 2)`.as('gap_to_target_rev_all'),

                    target_rev_ns: sql<number>`ROUND(SUM(c.target_rev_ns), 2)`.as('target_rev_ns'),
                    bill_amount_ns: sql<number>`ROUND(SUM(b.bill_amount_ns), 2)`.as('bill_amount_ns'),
                    bill_amount_ns_unpaid: sql<number>`ROUND(SUM(b.bill_amount_ns_unpaid), 2)`.as('bill_amount_ns_unpaid'),
                    ach_fm_rev_ns: sql<string>`CONCAT(ROUND(SUM(b.bill_amount_ns) / SUM(c.target_rev_ns) * 100, 2), '%')`.as('ach_fm_rev_ns'),
                    gap_to_target_rev_ns: sql<number>`ROUND((SUM(b.bill_amount_ns)) - SUM(c.target_rev_ns), 2)`.as('gap_to_target_rev_ns'),

                    target_rev_existing: sql<number>`ROUND(SUM(c.target_rev_existing), 2)`.as('target_rev_existing'),
                    bill_amount_existing: sql<number>`ROUND(SUM(b.bill_amount_existing), 2)`.as('bill_amount_existing'),
                    bill_amount_existing_unpaid: sql<number>`ROUND(SUM(b.bill_amount_existing_unpaid), 2)`.as('bill_amount_existing_unpaid'),
                    ach_fm_rev_existing: sql<string>`CONCAT(ROUND(SUM(b.bill_amount_existing) / SUM(c.target_rev_existing) * 100, 2), '%')`.as('ach_fm_rev_existing'),
                    gap_to_target_rev_existing: sql<number>`ROUND(SUM(b.bill_amount_existing)  - SUM(c.target_rev_existing), 2)`.as('gap_to_target_rev_existing'),

                    subs_0_6: summaryWok.subs_0_6,
                    subs_paid_0_6: summaryWok.subs_paid_0_6,
                    ach_subs_0_6: sql<string>`CONCAT(ROUND(b.subs_paid_0_6 / b.subs_0_6 * 100, 2), '%')`.as('ach_subs_0_6'),
                    subs_gt_6: summaryWok.subs_gt_6,
                    subs_paid_gt_6: summaryWok.subs_paid_gt_6,
                    ach_subs_paid_gt_6: sql<string>`CONCAT(ROUND(b.subs_paid_gt_6 / b.subs_gt_6 * 100, 2), '%')`.as('ach_subs_paid_gt_6'),

                    revenue_loss: sql<number>`ROUND((SUM(b.bill_amount_ns_unpaid) - SUM(b.bill_amount_all_unpaid)), 2)`.as('revenue_loss')
                })
                .from(wokTerritory)
                .leftJoin(summaryWok, sql`a.wok = b.wok`)
                .leftJoin(wokTargetRevenue, sql`a.wok = c.wok`)
                .groupBy(sql`a.wok`)

            if (branch && wok) {
                const [finalDataRevenue] = await Promise.all([
                    wokFinalQuery
                ])

                return c.json({ data: finalDataRevenue }, 200)
            }

            if (branch) {
                const [finalDataRevenue] = await Promise.all([
                    branchFinalQuery
                ])

                return c.json({ data: finalDataRevenue }, 200)
            }

            const [finalDataRevenue] = await Promise.all([
                regionalFinalQuery
            ])

            return c.json({ data: finalDataRevenue }, 200)
        })
    .get('/revenue-c3mr-v2', zValidator('query', z.object({ date: z.string().optional(), branch: z.string().optional(), wok: z.string().optional() })),
        async c => {
            const { date, branch, wok } = c.req.valid('query')
            const selectedDate = date ? parseISO(date) : subDays(new Date(), 2)

            const currMonth = format(selectedDate, 'MM')
            const currYear = format(selectedDate, 'yyyy')
            const lastDayOfSelectedMonth = endOfMonth(selectedDate);
            const isEndOfMonth = selectedDate.getDate() === lastDayOfSelectedMonth.getDate();

            const endOfCurrMonth = isEndOfMonth ? lastDayOfSelectedMonth : selectedDate;
            const endOfPrevMonth = isEndOfMonth ? endOfMonth(subMonths(selectedDate, 1)) : subMonths(selectedDate, 1);
            const endOfPrevYearSameMonth = isEndOfMonth ? endOfMonth(subYears(selectedDate, 1)) : subYears(selectedDate, 1);

            const currStartOfMonth = format(startOfMonth(selectedDate), 'yyyy-MM-dd')
            const currDate = format(selectedDate, 'yyyy-MM-dd')
            const prevDate = format(endOfPrevMonth, 'yyyy-MM-dd');
            const prevYearCurrDate = format(endOfPrevYearSameMonth, 'yyyy-MM-dd');
            const yyyyMM = format(selectedDate, 'yyyyMM')

            const branchTerritory = db
                .select({
                    branch: territoryHousehold.branch
                })
                .from(territoryHousehold)
                .where(branch ? and(
                    eq(territoryHousehold.regional, 'MALUKU DAN PAPUA'),
                    eq(territoryHousehold.branch, branch)) :
                    eq(territoryHousehold.regional, 'MALUKU DAN PAPUA')
                )
                .groupBy(sql`1`)
                .as('a')

            const branchTargetRevenue = db
                .select({
                    branch: targetRevenueC3mr.branch,
                    target_rev_all: sum(targetRevenueC3mr.rev_all).as('target_rev_all'),
                    target_rev_ns: sum(targetRevenueC3mr.rev_ns).as('target_rev_ns'),
                    target_rev_existing: sum(targetRevenueC3mr.rev_existing).as('target_rev_existing'),
                })
                .from(targetRevenueC3mr)
                .rightJoin(
                    db.selectDistinct({ branch: territoryHousehold.branch, wok: territoryHousehold.wok }).from(territoryHousehold).as('b'),
                    and(eq(targetRevenueC3mr.branch, territoryHousehold.branch), eq(targetRevenueC3mr.wok, territoryHousehold.wok))
                )
                .where(eq(targetRevenueC3mr, yyyyMM))
                .groupBy(sql`1`)
                .as('b')

            const summaryBranch = db
                .select({
                    branch: summaryHouseholdC3mrBranch.branch,
                    bill_amount_all: sql<number>`SUM(CASE WHEN billing_category = 'all' THEN ${summaryHouseholdC3mrBranch.bill_amount} END)`.as('bill_amount_all'),
                    bill_amount_ns: sql<number>`SUM(CASE WHEN billing_category = 'PSB' THEN ${summaryHouseholdC3mrBranch.bill_amount} END)`.as('bill_amount_ns'),
                    bill_amount_existing: sql<number>`SUM(CASE WHEN billing_category = 'EXISTING' THEN ${summaryHouseholdC3mrBranch.bill_amount} END)`.as('bill_amount_existing'),

                    subs_all: sql<number>`SUM(CASE WHEN billing_category = 'all' THEN ${summaryHouseholdC3mrBranch.subs} END)`.as('subs_all'),
                    subs_ns: sql<number>`SUM(CASE WHEN billing_category = 'PSB' THEN ${summaryHouseholdC3mrBranch.subs} END)`.as('subs_ns'),
                    subs_existing: sql<number>`SUM(CASE WHEN billing_category = 'EXISTING' THEN ${summaryHouseholdC3mrBranch.subs} END)`.as('subs_existing'),

                    amount_paid_all: sql<number>`CASE WHEN billing_category = 'all' THEN ${summaryHouseholdC3mrBranch.amount_paid} END`.as('amount_paid_all'),
                    amount_paid_ns: sql<number>`CASE WHEN billing_category = 'PSB' THEN ${summaryHouseholdC3mrBranch.amount_paid} END`.as('amount_paid_ns'),
                    amount_paid_existing: sql<number>`CASE WHEN billing_category = 'EXISTING' THEN ${summaryHouseholdC3mrBranch.amount_paid} END`.as('amount_paid_existing'),

                    subs_paid_all: sql<number>`SUM(CASE WHEN billing_category = 'all' THEN ${summaryHouseholdC3mrBranch.subs_paid} END)`.as('subs_paid_all'),
                    subs_paid_ns: sql<number>`SUM(CASE WHEN billing_category = 'PSB' THEN ${summaryHouseholdC3mrBranch.subs_paid} END)`.as('subs_paid_ns'),
                    subs_paid_existing: sql<number>`SUM(CASE WHEN billing_category = 'EXISTING' THEN ${summaryHouseholdC3mrBranch.subs_paid} END)`.as('subs_paid_existing'),

                    subs_unpaid_all: sql<number>`SUM(CASE WHEN billing_category = 'all' THEN ${summaryHouseholdC3mrBranch.subs_unpaid} END)`.as('subs_unpaid_all'),
                    subs_unpaid_ns: sql<number>`SUM(CASE WHEN billing_category = 'PSB' THEN ${summaryHouseholdC3mrBranch.subs_unpaid} END)`.as('subs_unpaid_ns'),
                    subs_unpaid_existing: sql<number>`SUM(CASE WHEN billing_category = 'EXISTING' THEN ${summaryHouseholdC3mrBranch.subs_unpaid} END)`.as('subs_unpaid_existing'),

                    amount_unpaid_all: sql<number>`SUM(CASE WHEN billing_category = 'all' THEN ${summaryHouseholdC3mrBranch.amount_unpaid} END)`.as('amount_unpaid_all'),
                    amount_unpaid_ns: sql<number>`SUM(CASE WHEN billing_category = 'PSB' THEN ${summaryHouseholdC3mrBranch.amount_unpaid} END)`.as('amount_unpaid_ns'),
                    amount_unpaid_existing: sql<number>`SUM(CASE WHEN billing_category = 'EXISTING' THEN ${summaryHouseholdC3mrBranch.amount_unpaid} END)`.as('amount_unpaid_existing'),
                })
                .from(summaryHouseholdC3mrBranch)
                .where(and(
                    eq(summaryHouseholdC3mrBranch.regional, 'MALUKU DAN PAPUA'),
                    eq(summaryHouseholdC3mrBranch.event_date, currDate),
                    eq(summaryHouseholdC3mrBranch.los_category, 'all'),
                ))
                .groupBy(sql`1`)
                .as('c')

            const branchC3mrByLos = db
                .select({
                    branch: summaryHouseholdC3mrBranch.branch,
                    subs_0_6: sql<number>`SUM(CASE WHEN los_category IN ('0-3 bulan', '4-6 bulan') THEN subs END)`.as('subs_0_6'),
                    subs_paid_0_6: sql<number>`SUM(CASE WHEN los_category IN ('0-3 bulan', '4-6 bulan') THEN subs_paid END)`.as('subs_paid_0_6'),
                    subs_gt_6: sql<number>`SUM(CASE WHEN los_category IN ('7-12 bulan', '12-24 bulan','>24 bulan') THEN subs END)`.as('subs_gt_6'),
                    subs_paid_gt_6: sql<number>`SUM(CASE WHEN los_category IN ('7-12 bulan', '12-24 bulan','>24 bulan') THEN subs_paid END)`.as('subs_paid_gt_6'),
                })
                .from(summaryHouseholdC3mrBranch)
                .where(branch ? and(
                    eq(summaryHouseholdC3mrBranch.regional, 'MALUKU DAN PAPUA'),
                    eq(summaryHouseholdC3mrBranch.branch, branch),
                    eq(summaryHouseholdC3mrBranch.billing_category, 'all')
                ) : eq(summaryHouseholdC3mrBranch.regional, 'MALUKU DAN PAPUA'))
                .groupBy(sql`1`)
                .as('f')

            const branchFinalQuery = db
                .select({
                    branch: branchTerritory.branch,
                    target_rev_all: sql<number>`ROUND(SUM(b.target_rev_all), 2)`.as('target_rev_all'),
                    rev_all: sql<number>`ROUND(SUM(c.bill_amount_all) / 1000000000, 2)`.as('rev_all'),
                    ach_fm_rev_all: sql<string>`CONCAT(ROUND((SUM(c.bill_amount_all)/1000000000/SUM(b.target_rev_all))*100, 2), '%')`.as('ach_fm_rev_all'),
                    gap_to_target_rev_all: sql<number>`ROUND((SUM(c.bill_amount_all) / 1000000000) - SUM(b.target_rev_all), 2)`.as('gap_to_target_rev_all'),

                    target_rev_ns: sql<number>`ROUND(SUM(b.target_rev_ns), 2)`.as('target_rev_ns'),
                    rev_ns: sql<number>`ROUND(SUM(c.bill_amount_ns) / 1000000000, 2)`.as('rev_ns'),
                    ach_fm_rev_ns: sql<string>`CONCAT(ROUND((SUM(c.bill_amount_ns)/1000000000/SUM(b.target_rev_ns))*100, 2), '%')`.as('ach_fm_rev_ns'),
                    gap_to_target_rev_ns: sql<number>`ROUND((SUM(c.bill_amount_ns) / 1000000000) - SUM(b.target_rev_ns), 2)`.as('gap_to_target_rev_ns'),

                    target_rev_existing: sql<number>`ROUND(SUM(b.target_rev_existing), 2)`.as('target_rev_existing'),
                    rev_existing: sql<number>`ROUND(SUM(c.bill_amount_existing) / 1000000000, 2)`.as('rev_existing'),
                    ach_fm_rev_existing: sql<string>`CONCAT(ROUND((SUM(c.bill_amount_existing)/1000000000/SUM(b.target_rev_existing))*100, 2), '%')`.as('ach_fm_rev_existing'),
                    gap_to_target_rev_existing: sql<number>`ROUND((SUM(c.bill_amount_existing) / 1000000000) - SUM(b.target_rev_existing), 2)`.as('gap_to_target_rev_existing'),

                    subs_0_6: branchC3mrByLos.subs_0_6,
                    subs_paid_0_6: branchC3mrByLos.subs_paid_0_6,
                    subs_gt_6: branchC3mrByLos.subs_gt_6,
                    subs_paid_gt_6: branchC3mrByLos.subs_paid_gt_6,
                })
                .from(branchTerritory)
                .leftJoin(branchTargetRevenue, eq(branchTerritory.branch, branchTargetRevenue.branch))
                .leftJoin(summaryBranch, eq(branchTerritory.branch, summaryBranch.branch))
                .leftJoin(branchC3mrByLos, eq(branchTerritory.branch, branchC3mrByLos.branch))
                .groupBy(sql`1`)

            const [finalDataRevenue] = await Promise.all([
                branchFinalQuery
            ])

            return c.json({ data: finalDataRevenue })
        })
    .get('/consolidation-all', zValidator('query', z.object({ date: z.string().optional(), branch: z.string().optional(), cluster: z.string().optional(), kabupaten: z.string().optional() })), async c => {
        const { date, branch, cluster, kabupaten } = c.req.valid('query')
        const selectedDate = date ? parseISO(date) : subDays(new Date(), 2)

        const endOfCurrMonth = endOfMonth(selectedDate);
        console.log({ selectedDate: format(endOfCurrMonth, 'yyyy-MM-dd') });

        const branchTerritory = db
            .select({
                branch: territoryArea4.branch
            })
            .from(territoryArea4)
            .where(branch ? eq(territoryArea4.branch, branch) : eq(territoryArea4.regional, 'PUMA'))
            .groupBy(sql`1`)
            .as('a')

        const branchRevenue = db
            .select()
            .from(summaryAllConsolidationBranch)
            .where(eq(summaryAllConsolidationBranch.tgl, format(endOfCurrMonth, 'yyyy-MM-dd')))
            .groupBy(summaryAllConsolidationBranch.branch)
            .as('b')

        const summaryBranch = db
            .select({
                territory: branchTerritory.branch,
                rev_all_m: sum(branchRevenue.rev_all_m).as('rev_all_m'),
                rev_all_m1: sum(branchRevenue.rev_all_m1).as('rev_all_m1'),
                rev_all_mom: branchRevenue.rev_all_mom,
                rev_all_y: sum(branchRevenue.rev_all_y).as('rev_all_y'),
                rev_all_y1: sum(branchRevenue.rev_all_y1).as('rev_all_y1'),
                rev_all_ytd: branchRevenue.rev_all_ytd,
                rev_fix_m: sum(branchRevenue.rev_fix_m).as('rev_fix_m'),
                rev_fix_m1: sum(branchRevenue.rev_fix_m1).as('rev_fix_m1'),
                rev_fix_mom: branchRevenue.rev_fix_mom,
                rev_fix_y: sum(branchRevenue.rev_fix_y).as("rev_fix_y"),
                rev_fix_y1: sum(branchRevenue.rev_fix_y1).as("rev_fix_y1"),
                rev_fix_ytd: branchRevenue.rev_fix_ytd,
                cons_m: sum(branchRevenue.consolidation_m).as('consolidation_m'),
                cons_m1: sum(branchRevenue.consolidation_m1).as('consolidation_m1'),
                cons_mom: branchRevenue.consolidation_mom,
                cons_y: sum(branchRevenue.consolidation_y).as('consolidation_y'),
                cons_y1: sum(branchRevenue.consolidation_y1).as('consolidation_y1'),
                cons_ytd: branchRevenue.consolidation_ytd
            })
            .from(branchTerritory)
            .leftJoin(branchRevenue, eq(branchTerritory.branch, branchRevenue.branch))
            .groupBy(sql`1`)

        const clusterHeader = db
            .selectDistinct({
                territory: sql<string>`'CLUSTER'`.as('territory'),
                rev_all_m: sql<string>`''`.as('rev_all_m'),
                rev_all_m1: sql<string>`''`.as('rev_all_m1'),
                rev_all_mom: sql<string>`''`.as('rev_all_mom'),
                rev_all_y: sql<string>`''`.as('rev_all_y'),
                rev_all_y1: sql<string>`''`.as('rev_all_y1'),
                rev_all_ytd: sql<string>`''`.as('rev_all_ytd'),
                rev_fix_m: sql<string>`''`.as('rev_fix_m'),
                rev_fix_m1: sql<string>`''`.as('rev_fix_m1'),
                rev_fix_mom: sql<string>`''`.as('rev_fix_mom'),
                rev_fix_y: sql<string>`''`.as('rev_fix_y'),
                rev_fix_y1: sql<string>`''`.as('rev_fix_y1'),
                rev_fix_ytd: sql<string>`''`.as('rev_fix_ytd'),
                cons_m: sql<string>`''`.as('cons_m'),
                cons_m1: sql<string>`''`.as('cons_m1'),
                cons_mom: sql<string>`''`.as('cons_mom'),
                cons_y: sql<string>`''`.as('cons_y'),
                cons_y1: sql<string>`''`.as('cons_y1'),
                cons_ytd: sql<string>`''`.as('cons_ytd')
            })
            .from(territoryArea4)

        const clusterTerritory = db
            .select({
                cluster: territoryArea4.cluster
            })
            .from(territoryArea4)
            .where(branch && cluster ? and(
                eq(territoryArea4.regional, 'PUMA'),
                eq(territoryArea4.cluster, cluster),
                eq(territoryArea4.branch, branch)
            ) : branch ? and(
                eq(territoryArea4.regional, 'PUMA'),
                eq(territoryArea4.branch, branch)
            ) : eq(territoryArea4.regional, 'PUMA'))
            .groupBy(sql`1`)
            .as('a')

        const clusterRevenue = db
            .select()
            .from(summaryAllConsolidationCluster)
            .where(eq(summaryAllConsolidationCluster.tgl, format(endOfCurrMonth, 'yyyy-MM-dd')))
            .groupBy(summaryAllConsolidationCluster.cluster)
            .as('b')

        const summaryCluster = db
            .select({
                territory: clusterTerritory.cluster,
                rev_all_m: sum(clusterRevenue.rev_all_m).as('rev_all_m'),
                rev_all_m1: sum(clusterRevenue.rev_all_m1).as('rev_all_m1'),
                rev_all_mom: clusterRevenue.rev_all_mom,
                rev_all_y: sum(clusterRevenue.rev_all_y).as('rev_all_y'),
                rev_all_y1: sum(clusterRevenue.rev_all_y1).as('rev_all_y1'),
                rev_all_ytd: clusterRevenue.rev_all_ytd,
                rev_fix_m: sum(clusterRevenue.rev_fix_m).as('rev_fix_m'),
                rev_fix_m1: sum(clusterRevenue.rev_fix_m1).as('rev_fix_m1'),
                rev_fix_mom: clusterRevenue.rev_fix_mom,
                rev_fix_y: sum(clusterRevenue.rev_fix_y).as("rev_fix_y"),
                rev_fix_y1: sum(clusterRevenue.rev_fix_y1).as("rev_fix_y1"),
                rev_fix_ytd: clusterRevenue.rev_fix_ytd,
                cons_m: sum(clusterRevenue.consolidation_m).as('consolidation_m'),
                cons_m1: sum(clusterRevenue.consolidation_m1).as('consolidation_m1'),
                cons_mom: clusterRevenue.consolidation_mom,
                cons_y: sum(clusterRevenue.consolidation_y).as('consolidation_y'),
                cons_y1: sum(clusterRevenue.consolidation_y1).as('consolidation_y1'),
                cons_ytd: clusterRevenue.consolidation_ytd
            })
            .from(clusterTerritory)
            .leftJoin(clusterRevenue, eq(clusterTerritory.cluster, clusterRevenue.cluster))
            .groupBy(sql`1`)

        const kabupatenHeader = db
            .selectDistinct({
                territory: sql<string>`'KABUPATEN'`.as('territory'),
                rev_all_m: sql<string>`''`.as('rev_all_m'),
                rev_all_m1: sql<string>`''`.as('rev_all_m1'),
                rev_all_mom: sql<string>`''`.as('rev_all_mom'),
                rev_all_y: sql<string>`''`.as('rev_all_y'),
                rev_all_y1: sql<string>`''`.as('rev_all_y1'),
                rev_all_ytd: sql<string>`''`.as('rev_all_ytd'),
                rev_fix_m: sql<string>`''`.as('rev_fix_m'),
                rev_fix_m1: sql<string>`''`.as('rev_fix_m1'),
                rev_fix_mom: sql<string>`''`.as('rev_fix_mom'),
                rev_fix_y: sql<string>`''`.as('rev_fix_y'),
                rev_fix_y1: sql<string>`''`.as('rev_fix_y1'),
                rev_fix_ytd: sql<string>`''`.as('rev_fix_ytd'),
                cons_m: sql<string>`''`.as('cons_m'),
                cons_m1: sql<string>`''`.as('cons_m1'),
                cons_mom: sql<string>`''`.as('cons_mom'),
                cons_y: sql<string>`''`.as('cons_y'),
                cons_y1: sql<string>`''`.as('cons_y1'),
                cons_ytd: sql<string>`''`.as('cons_ytd')
            })
            .from(territoryArea4)

        const kabupatenTerritory = db
            .select({
                kabupaten: territoryArea4.kabupaten
            })
            .from(territoryArea4)
            .where(branch && cluster && kabupaten ? and(
                eq(territoryArea4.regional, 'PUMA'),
                eq(territoryArea4.branch, branch),
                eq(territoryArea4.cluster, cluster),
                eq(territoryArea4.kabupaten, kabupaten)
            ) : branch && cluster ? and(
                eq(territoryArea4.regional, 'PUMA'),
                eq(territoryArea4.cluster, cluster),
                eq(territoryArea4.branch, branch)
            ) : branch ? and(
                eq(territoryArea4.regional, 'PUMA'),
                eq(territoryArea4.branch, branch)
            ) : eq(territoryArea4.regional, 'PUMA'))
            .groupBy(sql`1`)
            .as('a')

        const kabupatenRevenue = db
            .select()
            .from(summaryAllConsolidationKabupaten)
            .where(eq(summaryAllConsolidationKabupaten.tgl, format(endOfCurrMonth, 'yyyy-MM-dd')))
            .groupBy(summaryAllConsolidationKabupaten.kabupaten)
            .as('b')

        const summaryKabupaten = db
            .select({
                territory: kabupatenTerritory.kabupaten,
                rev_all_m: sum(kabupatenRevenue.rev_all_m).as('rev_all_m'),
                rev_all_m1: sum(kabupatenRevenue.rev_all_m1).as('rev_all_m1'),
                rev_all_mom: kabupatenRevenue.rev_all_mom,
                rev_all_y: sum(kabupatenRevenue.rev_all_y).as('rev_all_y'),
                rev_all_y1: sum(kabupatenRevenue.rev_all_y1).as('rev_all_y1'),
                rev_all_ytd: kabupatenRevenue.rev_all_ytd,
                rev_fix_m: sum(kabupatenRevenue.rev_fix_m).as('rev_fix_m'),
                rev_fix_m1: sum(kabupatenRevenue.rev_fix_m1).as('rev_fix_m1'),
                rev_fix_mom: kabupatenRevenue.rev_fix_mom,
                rev_fix_y: sum(kabupatenRevenue.rev_fix_y).as("rev_fix_y"),
                rev_fix_y1: sum(kabupatenRevenue.rev_fix_y1).as("rev_fix_y1"),
                rev_fix_ytd: kabupatenRevenue.rev_fix_ytd,
                cons_m: sum(kabupatenRevenue.consolidation_m).as('consolidation_m'),
                cons_m1: sum(kabupatenRevenue.consolidation_m1).as('consolidation_m1'),
                cons_mom: kabupatenRevenue.consolidation_mom,
                cons_y: sum(kabupatenRevenue.consolidation_y).as('consolidation_y'),
                cons_y1: sum(kabupatenRevenue.consolidation_y1).as('consolidation_y1'),
                cons_ytd: kabupatenRevenue.consolidation_ytd
            })
            .from(kabupatenTerritory)
            .leftJoin(kabupatenRevenue, eq(kabupatenTerritory.kabupaten, kabupatenRevenue.kabupaten))
            .groupBy(sql`1`)

        const [finalDataRevenue] = await Promise.all([
            unionAll(summaryBranch, clusterHeader, summaryCluster, kabupatenHeader, summaryKabupaten)
        ])

        return c.json(finalDataRevenue, 200)
    })
    .get('/consolidation-bb', zValidator('query', z.object({ date: z.string().optional(), branch: z.string().optional(), cluster: z.string().optional(), kabupaten: z.string().optional() })), async c => {
        const { date, branch, cluster, kabupaten } = c.req.valid('query')
        const selectedDate = date ? parseISO(date) : subDays(new Date(), 2)

        const currMonth = format(selectedDate, 'MM')
        const currYear = format(selectedDate, 'yyyy')
        const endOfCurrMonth = endOfMonth(selectedDate);
        console.log({ selectedDate: format(endOfCurrMonth, 'yyyy-MM-dd') });

        const branchTerritory = db
            .select({
                branch: territoryArea4.branch
            })
            .from(territoryArea4)
            .where(branch ? eq(territoryArea4.branch, branch) : eq(territoryArea4.regional, 'PUMA'))
            .groupBy(sql`1`)
            .as('a')

        const branchRevenue = db
            .select()
            .from(summaryBBConsolidationBranch)
            .where(eq(summaryBBConsolidationBranch.tgl, format(endOfCurrMonth, 'yyyy-MM-dd')))
            .groupBy(summaryBBConsolidationBranch.branch)
            .as('b')

        const summaryBranch = db
            .select({
                territory: branchTerritory.branch,
                rev_all_m: sum(branchRevenue.rev_bb_m).as('rev_bb_m'),
                rev_all_m1: sum(branchRevenue.rev_bb_m1).as('rev_bb_m1'),
                rev_all_mom: branchRevenue.rev_bb_mom,
                rev_all_y: sum(branchRevenue.rev_bb_y).as('rev_bb_y'),
                rev_all_y1: sum(branchRevenue.rev_bb_y1).as('rev_bb_y1'),
                rev_all_ytd: branchRevenue.rev_bb_ytd,
                rev_fix_m: sum(branchRevenue.rev_fix_m).as('rev_fix_m'),
                rev_fix_m1: sum(branchRevenue.rev_fix_m1).as('rev_fix_m1'),
                rev_fix_mom: branchRevenue.rev_fix_mom,
                rev_fix_y: sum(branchRevenue.rev_fix_y).as("rev_fix_y"),
                rev_fix_y1: sum(branchRevenue.rev_fix_y1).as("rev_fix_y1"),
                rev_fix_ytd: branchRevenue.rev_fix_ytd,
                cons_m: sum(branchRevenue.consolidation_m).as('consolidation_m'),
                cons_m1: sum(branchRevenue.consolidation_m1).as('consolidation_m1'),
                cons_mom: branchRevenue.consolidation_mom,
                cons_y: sum(branchRevenue.consolidation_y).as('consolidation_y'),
                cons_y1: sum(branchRevenue.consolidation_y1).as('consolidation_y1'),
                cons_ytd: branchRevenue.consolidation_ytd
            })
            .from(branchTerritory)
            .leftJoin(branchRevenue, eq(branchTerritory.branch, branchRevenue.branch))
            .groupBy(sql`1`)

        const clusterHeader = db
            .selectDistinct({
                territory: sql<string>`'CLUSTER'`.as('territory'),
                rev_all_m: sql<string>`''`.as('rev_bb_m'),
                rev_all_m1: sql<string>`''`.as('rev_bb_m1'),
                rev_all_mom: sql<string>`''`.as('rev_bb_mom'),
                rev_all_y: sql<string>`''`.as('rev_bb_y'),
                rev_all_y1: sql<string>`''`.as('rev_bb_y1'),
                rev_all_ytd: sql<string>`''`.as('rev_bb_ytd'),
                rev_fix_m: sql<string>`''`.as('rev_fix_m'),
                rev_fix_m1: sql<string>`''`.as('rev_fix_m1'),
                rev_fix_mom: sql<string>`''`.as('rev_fix_mom'),
                rev_fix_y: sql<string>`''`.as('rev_fix_y'),
                rev_fix_y1: sql<string>`''`.as('rev_fix_y1'),
                rev_fix_ytd: sql<string>`''`.as('rev_fix_ytd'),
                cons_m: sql<string>`''`.as('cons_m'),
                cons_m1: sql<string>`''`.as('cons_m1'),
                cons_mom: sql<string>`''`.as('cons_mom'),
                cons_y: sql<string>`''`.as('cons_y'),
                cons_y1: sql<string>`''`.as('cons_y1'),
                cons_ytd: sql<string>`''`.as('cons_ytd')
            })
            .from(territoryArea4)

        const clusterTerritory = db
            .select({
                cluster: territoryArea4.cluster
            })
            .from(territoryArea4)
            .where(branch && cluster ? and(
                eq(territoryArea4.regional, 'PUMA'),
                eq(territoryArea4.cluster, cluster),
                eq(territoryArea4.branch, branch)
            ) : branch ? and(
                eq(territoryArea4.regional, 'PUMA'),
                eq(territoryArea4.branch, branch)
            ) : eq(territoryArea4.regional, 'PUMA'))
            .groupBy(sql`1`)
            .as('a')

        const clusterRevenue = db
            .select()
            .from(summaryBBConsolidationCluster)
            .where(eq(summaryBBConsolidationCluster.tgl, format(endOfCurrMonth, 'yyyy-MM-dd')))
            .groupBy(summaryBBConsolidationCluster.cluster)
            .as('b')

        const summaryCluster = db
            .select({
                territory: clusterTerritory.cluster,
                rev_all_m: sum(clusterRevenue.rev_bb_m).as('rev_bb_m'),
                rev_all_m1: sum(clusterRevenue.rev_bb_m1).as('rev_bb_m1'),
                rev_all_mom: clusterRevenue.rev_bb_mom,
                rev_all_y: sum(clusterRevenue.rev_bb_y).as('rev_bb_y'),
                rev_all_y1: sum(clusterRevenue.rev_bb_y1).as('rev_bb_y1'),
                rev_all_ytd: clusterRevenue.rev_bb_ytd,
                rev_fix_m: sum(clusterRevenue.rev_fix_m).as('rev_fix_m'),
                rev_fix_m1: sum(clusterRevenue.rev_fix_m1).as('rev_fix_m1'),
                rev_fix_mom: clusterRevenue.rev_fix_mom,
                rev_fix_y: sum(clusterRevenue.rev_fix_y).as("rev_fix_y"),
                rev_fix_y1: sum(clusterRevenue.rev_fix_y1).as("rev_fix_y1"),
                rev_fix_ytd: clusterRevenue.rev_fix_ytd,
                cons_m: sum(clusterRevenue.consolidation_m).as('consolidation_m'),
                cons_m1: sum(clusterRevenue.consolidation_m1).as('consolidation_m1'),
                cons_mom: clusterRevenue.consolidation_mom,
                cons_y: sum(clusterRevenue.consolidation_y).as('consolidation_y'),
                cons_y1: sum(clusterRevenue.consolidation_y1).as('consolidation_y1'),
                cons_ytd: clusterRevenue.consolidation_ytd
            })
            .from(clusterTerritory)
            .leftJoin(clusterRevenue, eq(clusterTerritory.cluster, clusterRevenue.cluster))
            .groupBy(sql`1`)

        const kabupatenHeader = db
            .selectDistinct({
                territory: sql<string>`'KABUPATEN'`.as('territory'),
                rev_all_m: sql<string>`''`.as('rev_bb_m'),
                rev_all_m1: sql<string>`''`.as('rev_bb_m1'),
                rev_all_mom: sql<string>`''`.as('rev_bb_mom'),
                rev_all_y: sql<string>`''`.as('rev_bb_y'),
                rev_all_y1: sql<string>`''`.as('rev_bb_y1'),
                rev_all_ytd: sql<string>`''`.as('rev_bb_ytd'),
                rev_fix_m: sql<string>`''`.as('rev_fix_m'),
                rev_fix_m1: sql<string>`''`.as('rev_fix_m1'),
                rev_fix_mom: sql<string>`''`.as('rev_fix_mom'),
                rev_fix_y: sql<string>`''`.as('rev_fix_y'),
                rev_fix_y1: sql<string>`''`.as('rev_fix_y1'),
                rev_fix_ytd: sql<string>`''`.as('rev_fix_ytd'),
                cons_m: sql<string>`''`.as('cons_m'),
                cons_m1: sql<string>`''`.as('cons_m1'),
                cons_mom: sql<string>`''`.as('cons_mom'),
                cons_y: sql<string>`''`.as('cons_y'),
                cons_y1: sql<string>`''`.as('cons_y1'),
                cons_ytd: sql<string>`''`.as('cons_ytd')
            })
            .from(territoryArea4)

        const kabupatenTerritory = db
            .select({
                kabupaten: territoryArea4.kabupaten
            })
            .from(territoryArea4)
            .where(branch && cluster && kabupaten ? and(
                eq(territoryArea4.regional, 'PUMA'),
                eq(territoryArea4.branch, branch),
                eq(territoryArea4.cluster, cluster),
                eq(territoryArea4.kabupaten, kabupaten)
            ) : branch && cluster ? and(
                eq(territoryArea4.regional, 'PUMA'),
                eq(territoryArea4.cluster, cluster),
                eq(territoryArea4.branch, branch)
            ) : branch ? and(
                eq(territoryArea4.regional, 'PUMA'),
                eq(territoryArea4.branch, branch)
            ) : eq(territoryArea4.regional, 'PUMA'))
            .groupBy(sql`1`)
            .as('a')

        const kabupatenRevenue = db
            .select()
            .from(summaryBBConsolidationKabupaten)
            .where(eq(summaryBBConsolidationKabupaten.tgl, format(endOfCurrMonth, 'yyyy-MM-dd')))
            .groupBy(summaryBBConsolidationKabupaten.kabupaten)
            .as('b')

        const summaryKabupaten = db
            .select({
                territory: kabupatenTerritory.kabupaten,
                rev_all_m: sum(kabupatenRevenue.rev_bb_m).as('rev_bb_m'),
                rev_all_m1: sum(kabupatenRevenue.rev_bb_m1).as('rev_bb_m1'),
                rev_all_mom: kabupatenRevenue.rev_bb_mom,
                rev_all_y: sum(kabupatenRevenue.rev_bb_y).as('rev_bb_y'),
                rev_all_y1: sum(kabupatenRevenue.rev_bb_y1).as('rev_bb_y1'),
                rev_all_ytd: kabupatenRevenue.rev_bb_ytd,
                rev_fix_m: sum(kabupatenRevenue.rev_fix_m).as('rev_fix_m'),
                rev_fix_m1: sum(kabupatenRevenue.rev_fix_m1).as('rev_fix_m1'),
                rev_fix_mom: kabupatenRevenue.rev_fix_mom,
                rev_fix_y: sum(kabupatenRevenue.rev_fix_y).as("rev_fix_y"),
                rev_fix_y1: sum(kabupatenRevenue.rev_fix_y1).as("rev_fix_y1"),
                rev_fix_ytd: kabupatenRevenue.rev_fix_ytd,
                cons_m: sum(kabupatenRevenue.consolidation_m).as('consolidation_m'),
                cons_m1: sum(kabupatenRevenue.consolidation_m1).as('consolidation_m1'),
                cons_mom: kabupatenRevenue.consolidation_mom,
                cons_y: sum(kabupatenRevenue.consolidation_y).as('consolidation_y'),
                cons_y1: sum(kabupatenRevenue.consolidation_y1).as('consolidation_y1'),
                cons_ytd: kabupatenRevenue.consolidation_ytd
            })
            .from(kabupatenTerritory)
            .leftJoin(kabupatenRevenue, eq(kabupatenTerritory.kabupaten, kabupatenRevenue.kabupaten))
            .groupBy(sql`1`)

        const [finalDataRevenue] = await Promise.all([
            unionAll(summaryBranch, clusterHeader, summaryCluster, kabupatenHeader, summaryKabupaten)
        ])

        return c.json(finalDataRevenue, 200)
    })
    .get('/consolidation-payload', zValidator('query', z.object({ date: z.string().optional(), branch: z.string().optional(), cluster: z.string().optional(), kabupaten: z.string().optional() })), async c => {
        const { date, branch, cluster, kabupaten } = c.req.valid('query')
        const selectedDate = date ? parseISO(date) : subDays(new Date(), 2)

        const currMonth = format(selectedDate, 'MM')
        const currYear = format(selectedDate, 'yyyy')
        const endOfCurrMonth = endOfMonth(selectedDate);
        console.log({ selectedDate: format(endOfCurrMonth, 'yyyy-MM-dd') });

        const branchTerritory = db
            .select({
                branch: territoryArea4.branch
            })
            .from(territoryArea4)
            .where(branch ? eq(territoryArea4.branch, branch) : eq(territoryArea4.regional, 'PUMA'))
            .groupBy(sql`1`)
            .as('a')

        const branchRevenue = db
            .select()
            .from(summaryPayloadConsolidationBranch)
            .where(eq(summaryPayloadConsolidationBranch.tgl, format(endOfCurrMonth, 'yyyy-MM-dd')))
            .groupBy(summaryPayloadConsolidationBranch.branch)
            .as('b')

        const summaryBranch = db
            .select({
                territory: branchTerritory.branch,
                rev_all_m: sum(branchRevenue.payload_mobile_m).as('payload_mobile_m'),
                rev_all_m1: sum(branchRevenue.payload_mobile_m1).as('payload_mobile_m1'),
                rev_all_mom: branchRevenue.payload_mobile_mom,
                rev_all_y: sum(branchRevenue.payload_mobile_y).as('payload_mobile_y'),
                rev_all_y1: sum(branchRevenue.payload_mobile_y1).as('payload_mobile_y1'),
                rev_all_ytd: branchRevenue.payload_mobile_ytd,
                rev_fix_m: sum(branchRevenue.payload_fix_m).as('payload_fix_m'),
                rev_fix_m1: sum(branchRevenue.payload_fix_m1).as('payload_fix_m1'),
                rev_fix_mom: branchRevenue.payload_fix_mom,
                rev_fix_y: sum(branchRevenue.payload_fix_y).as("payload_fix_y"),
                rev_fix_y1: sum(branchRevenue.payload_fix_y1).as("payload_fix_y1"),
                rev_fix_ytd: branchRevenue.payload_fix_ytd,
                cons_m: sum(branchRevenue.consolidation_m).as('consolidation_m'),
                cons_m1: sum(branchRevenue.consolidation_m1).as('consolidation_m1'),
                cons_mom: branchRevenue.consolidation_mom,
                cons_y: sum(branchRevenue.consolidation_y).as('consolidation_y'),
                cons_y1: sum(branchRevenue.consolidation_y1).as('consolidation_y1'),
                cons_ytd: branchRevenue.consolidation_ytd
            })
            .from(branchTerritory)
            .leftJoin(branchRevenue, eq(branchTerritory.branch, branchRevenue.branch))
            .groupBy(sql`1`)

        const clusterHeader = db
            .selectDistinct({
                territory: sql<string>`'CLUSTER'`.as('territory'),
                rev_all_m: sql<string>`''`.as('payload_mobile_m'),
                rev_all_m1: sql<string>`''`.as('payload_mobile_m1'),
                rev_all_mom: sql<string>`''`.as('payload_mobile_mom'),
                rev_all_y: sql<string>`''`.as('payload_mobile_y'),
                rev_all_y1: sql<string>`''`.as('payload_mobile_y1'),
                rev_all_ytd: sql<string>`''`.as('payload_mobile_ytd'),
                rev_fix_m: sql<string>`''`.as('payload_fix_m'),
                rev_fix_m1: sql<string>`''`.as('payload_fix_m1'),
                rev_fix_mom: sql<string>`''`.as('payload_fix_mom'),
                rev_fix_y: sql<string>`''`.as('payload_fix_y'),
                rev_fix_y1: sql<string>`''`.as('payload_fix_y1'),
                rev_fix_ytd: sql<string>`''`.as('payload_fix_ytd'),
                cons_m: sql<string>`''`.as('cons_m'),
                cons_m1: sql<string>`''`.as('cons_m1'),
                cons_mom: sql<string>`''`.as('cons_mom'),
                cons_y: sql<string>`''`.as('cons_y'),
                cons_y1: sql<string>`''`.as('cons_y1'),
                cons_ytd: sql<string>`''`.as('cons_ytd')
            })
            .from(territoryArea4)

        const clusterTerritory = db
            .select({
                cluster: territoryArea4.cluster
            })
            .from(territoryArea4)
            .where(branch && cluster ? and(
                eq(territoryArea4.regional, 'PUMA'),
                eq(territoryArea4.cluster, cluster),
                eq(territoryArea4.branch, branch)
            ) : branch ? and(
                eq(territoryArea4.regional, 'PUMA'),
                eq(territoryArea4.branch, branch)
            ) : eq(territoryArea4.regional, 'PUMA'))
            .groupBy(sql`1`)
            .as('a')

        const clusterRevenue = db
            .select()
            .from(summaryPayloadConsolidationCluster)
            .where(eq(summaryPayloadConsolidationCluster.tgl, format(endOfCurrMonth, 'yyyy-MM-dd')))
            .groupBy(summaryPayloadConsolidationCluster.cluster)
            .as('b')

        const summaryCluster = db
            .select({
                territory: clusterTerritory.cluster,
                rev_all_m: sum(clusterRevenue.payload_mobile_m).as('payload_mobile_m'),
                rev_all_m1: sum(clusterRevenue.payload_mobile_m1).as('payload_mobile_m1'),
                rev_all_mom: clusterRevenue.payload_mobile_mom,
                rev_all_y: sum(clusterRevenue.payload_mobile_y).as('payload_mobile_y'),
                rev_all_y1: sum(clusterRevenue.payload_mobile_y1).as('payload_mobile_y1'),
                rev_all_ytd: clusterRevenue.payload_mobile_ytd,
                rev_fix_m: sum(clusterRevenue.payload_fix_m).as('payload_fix_m'),
                rev_fix_m1: sum(clusterRevenue.payload_fix_m1).as('payload_fix_m1'),
                rev_fix_mom: clusterRevenue.payload_fix_mom,
                rev_fix_y: sum(clusterRevenue.payload_fix_y).as("payload_fix_y"),
                rev_fix_y1: sum(clusterRevenue.payload_fix_y1).as("payload_fix_y1"),
                rev_fix_ytd: clusterRevenue.payload_fix_ytd,
                cons_m: sum(clusterRevenue.consolidation_m).as('consolidation_m'),
                cons_m1: sum(clusterRevenue.consolidation_m1).as('consolidation_m1'),
                cons_mom: clusterRevenue.consolidation_mom,
                cons_y: sum(clusterRevenue.consolidation_y).as('consolidation_y'),
                cons_y1: sum(clusterRevenue.consolidation_y1).as('consolidation_y1'),
                cons_ytd: clusterRevenue.consolidation_ytd
            })
            .from(clusterTerritory)
            .leftJoin(clusterRevenue, eq(clusterTerritory.cluster, clusterRevenue.cluster))
            .groupBy(sql`1`)

        const kabupatenHeader = db
            .selectDistinct({
                territory: sql<string>`'KABUPATEN'`.as('territory'),
                rev_all_m: sql<string>`''`.as('payload_mobile_m'),
                rev_all_m1: sql<string>`''`.as('payload_mobile_m1'),
                rev_all_mom: sql<string>`''`.as('payload_mobile_mom'),
                rev_all_y: sql<string>`''`.as('payload_mobile_y'),
                rev_all_y1: sql<string>`''`.as('payload_mobile_y1'),
                rev_all_ytd: sql<string>`''`.as('payload_mobile_ytd'),
                rev_fix_m: sql<string>`''`.as('payload_fix_m'),
                rev_fix_m1: sql<string>`''`.as('payload_fix_m1'),
                rev_fix_mom: sql<string>`''`.as('payload_fix_mom'),
                rev_fix_y: sql<string>`''`.as('payload_fix_y'),
                rev_fix_y1: sql<string>`''`.as('payload_fix_y1'),
                rev_fix_ytd: sql<string>`''`.as('payload_fix_ytd'),
                cons_m: sql<string>`''`.as('cons_m'),
                cons_m1: sql<string>`''`.as('cons_m1'),
                cons_mom: sql<string>`''`.as('cons_mom'),
                cons_y: sql<string>`''`.as('cons_y'),
                cons_y1: sql<string>`''`.as('cons_y1'),
                cons_ytd: sql<string>`''`.as('cons_ytd')
            })
            .from(territoryArea4)

        const kabupatenTerritory = db
            .select({
                kabupaten: territoryArea4.kabupaten
            })
            .from(territoryArea4)
            .where(branch && cluster && kabupaten ? and(
                eq(territoryArea4.regional, 'PUMA'),
                eq(territoryArea4.branch, branch),
                eq(territoryArea4.cluster, cluster),
                eq(territoryArea4.kabupaten, kabupaten)
            ) : branch && cluster ? and(
                eq(territoryArea4.regional, 'PUMA'),
                eq(territoryArea4.cluster, cluster),
                eq(territoryArea4.branch, branch)
            ) : branch ? and(
                eq(territoryArea4.regional, 'PUMA'),
                eq(territoryArea4.branch, branch)
            ) : eq(territoryArea4.regional, 'PUMA'))
            .groupBy(sql`1`)
            .as('a')

        const kabupatenRevenue = db
            .select()
            .from(summaryPayloadConsolidationKabupaten)
            .where(eq(summaryPayloadConsolidationKabupaten.tgl, format(endOfCurrMonth, 'yyyy-MM-dd')))
            .groupBy(summaryPayloadConsolidationKabupaten.kabupaten)
            .as('b')

        const summaryKabupaten = db
            .select({
                territory: kabupatenTerritory.kabupaten,
                rev_all_m: sum(kabupatenRevenue.payload_mobile_m).as('payload_mobile_m'),
                rev_all_m1: sum(kabupatenRevenue.payload_mobile_m1).as('payload_mobile_m1'),
                rev_all_mom: kabupatenRevenue.payload_mobile_mom,
                rev_all_y: sum(kabupatenRevenue.payload_mobile_y).as('payload_mobile_y'),
                rev_all_y1: sum(kabupatenRevenue.payload_mobile_y1).as('payload_mobile_y1'),
                rev_all_ytd: kabupatenRevenue.payload_mobile_ytd,
                rev_fix_m: sum(kabupatenRevenue.payload_fix_m).as('payload_fix_m'),
                rev_fix_m1: sum(kabupatenRevenue.payload_fix_m1).as('payload_fix_m1'),
                rev_fix_mom: kabupatenRevenue.payload_fix_mom,
                rev_fix_y: sum(kabupatenRevenue.payload_fix_y).as("payload_fix_y"),
                rev_fix_y1: sum(kabupatenRevenue.payload_fix_y1).as("payload_fix_y1"),
                rev_fix_ytd: kabupatenRevenue.payload_fix_ytd,
                cons_m: sum(kabupatenRevenue.consolidation_m).as('consolidation_m'),
                cons_m1: sum(kabupatenRevenue.consolidation_m1).as('consolidation_m1'),
                cons_mom: kabupatenRevenue.consolidation_mom,
                cons_y: sum(kabupatenRevenue.consolidation_y).as('consolidation_y'),
                cons_y1: sum(kabupatenRevenue.consolidation_y1).as('consolidation_y1'),
                cons_ytd: kabupatenRevenue.consolidation_ytd
            })
            .from(kabupatenTerritory)
            .leftJoin(kabupatenRevenue, eq(kabupatenTerritory.kabupaten, kabupatenRevenue.kabupaten))
            .groupBy(sql`1`)

        const [finalDataRevenue] = await Promise.all([
            unionAll(summaryBranch, clusterHeader, summaryCluster, kabupatenHeader, summaryKabupaten)
        ])

        return c.json(finalDataRevenue, 200)
    })
    .get('/hh-max-dates', async c => {
        const selectedDate = subDays(new Date(), 2)

        const currMonth = format(selectedDate, 'MM')
        const currYear = format(selectedDate, 'yyyy')
        const lastDayOfSelectedMonth = endOfMonth(selectedDate);
        const isEndOfMonth = selectedDate.getDate() === lastDayOfSelectedMonth.getDate();

        const ihOrderingDetailOrder = dynamicIhOrderingDetailOrderTable(currYear, currMonth)

        const branchTerritory = db
            .selectDistinct({
                branch: territoryArea4.branch
            })
            .from(territoryArea4)
            .where(eq(territoryArea4.regional, 'PUMA'))
            .as('branch_territory')

        const orderingDate = db
            .select({
                branch: ihOrderingDetailOrder.branch,
                ordering_max_date: sql<string>`MAX(event_date)`.as('ordering_max_date')
            })
            .from(ihOrderingDetailOrder)
            .where(eq(ihOrderingDetailOrder.region, 'MALUKU DAN PAPUA'))
            .groupBy(sql`1`)
            .as('ihOrderingDetailOrder')

        const allDates = db
            .select({
                cons_max_date: sql<string>`MAX(${summaryAllConsolidationBranch.tgl})`.as('cons_max_date'),
                ioreps_max_date: sql<string>`MAX(${summaryIoRePsBranch.event_date})`.as('ioreps_max_date'),
                ordering_max_date: sql<string>`${orderingDate.ordering_max_date}`.as('ordering_max_date')
            })
            .from(branchTerritory)
            .leftJoin(summaryAllConsolidationBranch, eq(branchTerritory.branch, summaryAllConsolidationBranch.branch))
            .leftJoin(summaryIoRePsBranch, eq(branchTerritory.branch, summaryIoRePsBranch.branch))
            .leftJoin(orderingDate, eq(branchTerritory.branch, orderingDate.branch))

        const [all] = await Promise.all([
            allDates
        ])

        return c.json({ data: all }, 200)
    })

export default app