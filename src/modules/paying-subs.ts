import { endOfMonth, format, getDaysInMonth, subDays, subMonths, subYears } from "date-fns";
import { and, eq, inArray, max, sql, sum } from "drizzle-orm";
import { Hono } from "hono";
import { z } from "zod";
import { zValidator } from "@hono/zod-validator";

import { db } from "@/db";
import { feiTargetPuma, summaryRevAllKabupaten, summaryRgbHqKabupaten, summaryRgbHqRegional, summaryRgbHqBranch, summaryRgbHqCluster, summaryRevAllRegional, summaryRevAllBranch, summaryRevAllCluster, summaryRevAllSubbranch } from "@/db/schema/v_honai_puma";
import { territoryArea4 } from "@/db/schema/puma_2025";
import { unionAll } from "drizzle-orm/mysql-core";

const app = new Hono()
    .get('/paying-los-all', zValidator('query', z.object({ date: z.string().optional(), branch: z.string().optional(), subbranch: z.string().optional(), cluster: z.string().optional(), kabupaten: z.string().optional() })),
        async c => {
            const { date, branch, subbranch, cluster, kabupaten } = c.req.valid('query')
            const selectedDate = date ? new Date(date) : subDays(new Date(), 2)

            const lastDayOfSelectedMonth = endOfMonth(selectedDate);
            const isEndOfMonth = selectedDate.getDate() === lastDayOfSelectedMonth.getDate();

            const endOfCurrMonth = isEndOfMonth ? lastDayOfSelectedMonth : selectedDate;
            const endOfPrevMonth = isEndOfMonth ? endOfMonth(subMonths(selectedDate, 1)) : subMonths(selectedDate, 1);
            const endOfPrevYearSameMonth = isEndOfMonth ? endOfMonth(subYears(selectedDate, 1)) : subYears(selectedDate, 1);

            const currDate = format(selectedDate, 'yyyy-MM-dd')
            const prevDate = format(endOfPrevMonth, 'yyyy-MM-dd');
            const prevYearCurrDate = format(endOfPrevYearSameMonth, 'yyyy-MM-dd');
            const yyyyMM = format(selectedDate, 'yyyyMM')
            const daysInMonth = getDaysInMonth(selectedDate)
            const today = Number(format(selectedDate, 'd'))

            const regionalTerritory = db
                .select({
                    regional: territoryArea4.regional,
                })
                .from(territoryArea4)
                .where(eq(territoryArea4.regional, 'PUMA'))
                .groupBy(territoryArea4.regional)
                .as('a')

            const rgbRegional = db
                .select({
                    regional: summaryRgbHqRegional.regional,
                    cbLastYear: summaryRgbHqRegional.cb_m12,
                    cbLastMonth: summaryRgbHqRegional.cb_m1,
                    mtdCb: summaryRgbHqRegional.cb_mtd,
                    rgbLastYear: summaryRgbHqRegional.rgb_m12,
                    rgbLastMonth: summaryRgbHqRegional.rgb_m1,
                    mtdRgb: summaryRgbHqRegional.rgb_mtd,
                    rgbDataLastYear: summaryRgbHqRegional.rgb_data_m12,
                    rgbDataLastMonth: summaryRgbHqRegional.rgb_data_m1,
                    mtdRgbData: summaryRgbHqRegional.rgb_data_mtd,
                    rgbDigitalLastYear: summaryRgbHqRegional.rgb_digital_m12,
                    rgbDigitalLastMonth: summaryRgbHqRegional.rgb_digital_m1,
                    mtdRgbDigital: summaryRgbHqRegional.rgb_digital_mtd,
                    payloadLastYear: summaryRgbHqRegional.payload_user_m12,
                    payloadLastMonth: summaryRgbHqRegional.payload_user_m1,
                    mtdPayload: summaryRgbHqRegional.payload_user_mtd
                })
                .from(summaryRgbHqRegional)
                .where(and(
                    eq(summaryRgbHqRegional.event_date, currDate),
                    eq(summaryRgbHqRegional.regional, 'PUMA'),
                    eq(summaryRgbHqRegional.catlos, '1.0-1 mo'),
                    eq(summaryRgbHqRegional.brand, 'all')
                ))
                .groupBy(summaryRgbHqRegional.regional)
                .as('b')

            const regionalTargetRevenue = db
                .select({
                    regional: territoryArea4.regional,
                    target_paying_los_0_1: sum(feiTargetPuma.paying_los_0_1).as('target_paying_los_0_1')
                })
                .from(feiTargetPuma)
                .rightJoin(territoryArea4, eq(feiTargetPuma.territory, territoryArea4.kabupaten))
                .where(eq(feiTargetPuma.periode, yyyyMM))
                .groupBy(territoryArea4.regional)
                .as('c')

            const summaryRevRegional = db
                .select({
                    regional: summaryRevAllRegional.regional,
                    revAllM: summaryRevAllRegional.rev_all_m,
                    revAllM1: sql<number>`SUM(CASE WHEN ${summaryRevAllRegional.tgl} = ${prevDate} THEN ${summaryRevAllRegional.rev_all_m} END)`.as('rev_all_m1'),
                    revAllM12: sql<number>`SUM(CASE WHEN ${summaryRevAllRegional.tgl} = ${prevYearCurrDate} THEN ${summaryRevAllRegional.rev_all_m} END)`.as('rev_all_m12'),

                    revBbM: summaryRevAllRegional.rev_bb_m,
                    revBbM1: sql<number>`SUM(CASE WHEN ${summaryRevAllRegional.tgl} = ${prevDate} THEN ${summaryRevAllRegional.rev_bb_m} END)`.as('rev_bb_m1'),
                    revBbM12: sql<number>`SUM(CASE WHEN ${summaryRevAllRegional.tgl} = ${prevYearCurrDate} THEN ${summaryRevAllRegional.rev_bb_m} END)`.as('rev_bb_m12'),

                    revVoiceM: summaryRevAllRegional.rev_voice_m,
                    revVoiceM1: sql<number>`SUM(CASE WHEN ${summaryRevAllRegional.tgl} = ${prevDate} THEN ${summaryRevAllRegional.rev_voice_m} END)`.as('rev_voice_m1'),
                    revVoiceM12: sql<number>`SUM(CASE WHEN ${summaryRevAllRegional.tgl} = ${prevYearCurrDate} THEN ${summaryRevAllRegional.rev_voice_m} END)`.as('rev_voice_m12'),

                    revDigitalM: summaryRevAllRegional.rev_digital_m,
                    revDigitalM1: sql<number>`SUM(CASE WHEN ${summaryRevAllRegional.tgl} = ${prevDate} THEN ${summaryRevAllRegional.rev_digital_m} END)`.as('rev_digital_m1'),
                    revDigitalM12: sql<number>`SUM(CASE WHEN ${summaryRevAllRegional.tgl} = ${prevYearCurrDate} THEN ${summaryRevAllRegional.rev_digital_m} END)`.as('rev_digital_m12')
                })
                .from(summaryRevAllRegional)
                .where(and(
                    eq(summaryRevAllRegional.regional, 'PUMA'),
                    eq(summaryRevAllRegional.newAbcStrate, 'all')
                ))
                .groupBy(summaryRevAllRegional.regional)
                .as('d')

            const revenueRegional = db
                .select({
                    name: regionalTerritory.regional,
                    target_rgb_all: sql<number>`ROUND(SUM(${regionalTargetRevenue.target_paying_los_0_1}),0)`.as('target_rgb_all'),
                    mtd_rgb: sql<number>`ROUND(SUM(${rgbRegional.mtdRgb}),0)`.as('mtd_rgb'),
                    mom_rgb: sql<string>`CONCAT(ROUND(((SUM(${rgbRegional.mtdRgb})/SUM(${rgbRegional.rgbLastMonth}))-1)*100,2),'%')`.as('mom_rgb'),
                    yoy_rgb: sql<string>`CONCAT(ROUND(((SUM(${rgbRegional.mtdRgb})/SUM(${rgbRegional.rgbLastYear}))-1)*100,2),'%')`.as('yoy_rgb'),

                    ach_target_fm_rgb: sql<string>`CONCAT(ROUND((SUM(${rgbRegional.mtdRgb})/SUM(${regionalTargetRevenue.target_paying_los_0_1}))*100,2),'%')`.as('ach_target_fm_rgb'),
                    drr_rgb: sql<string>`CONCAT(ROUND((SUM(${rgbRegional.mtdRgb})/(${today}/${daysInMonth}*(SUM(${regionalTargetRevenue.target_paying_los_0_1}))))*100,2),'%')`.as('drr_rgb'),
                    gap_to_target_rgb: sql<string>`ROUND((COALESCE(SUM(${rgbRegional.mtdRgb}) - SUM(${regionalTargetRevenue.target_paying_los_0_1}),0)),0)`.as('gap_to_target_rgb'),
                    penetration_to_cb: sql<string>`CONCAT(ROUND((SUM(${rgbRegional.mtdRgb})/SUM(${rgbRegional.mtdCb})*100),2),'%')`.as('penetration_to_cb'),

                    mtd_rgb_data: sql<number>`ROUND(SUM(${rgbRegional.mtdRgbData}),0)`.as('mtd_rgb_data'),
                    mom_rgb_data: sql<string>`CONCAT(ROUND((((SUM(${rgbRegional.mtdRgbData})/SUM(${rgbRegional.rgbDataLastMonth}))-1)*100),2),'%')`.as('mom_rgb_data'),
                    yoy_rgb_data: sql<string>`CONCAT(ROUND((((SUM(${rgbRegional.mtdRgbData})/SUM(${rgbRegional.rgbDataLastYear}))-1)*100),2),'%')`.as('yoy_rgb_data'),

                    mtd_rgb_digital: sql<number>`ROUND(SUM(${rgbRegional.mtdRgbDigital}),0)`.as('mtd_rgb_digital'),
                    mom_rgb_digital: sql<string>`CONCAT(ROUND((((SUM(${rgbRegional.mtdRgbDigital})/SUM(${rgbRegional.rgbDigitalLastMonth}))-1)*100),2),'%')`.as('mom_rgb_digital'),
                    yoy_rgb_digital: sql<string>`CONCAT(ROUND((((SUM(${rgbRegional.mtdRgbDigital})/SUM(${rgbRegional.rgbDigitalLastYear}))-1)*100),2),'%')`.as('yoy_rgb_digital'),

                    mtd_arpu: sql<number>`ROUND(SUM(${summaryRevRegional.revAllM})/SUM(${rgbRegional.mtdRgb}),0)`.as('mtd_arpu'),
                    mom_arpu: sql<string>`CONCAT(ROUND((((SUM(${summaryRevRegional.revAllM})/SUM(${rgbRegional.mtdRgb}))/(SUM(${summaryRevRegional.revAllM1})/SUM(${rgbRegional.rgbDataLastMonth})))-1)*100,2),'%')`.as('mom_arpu'),
                    yoy_arpu: sql<string>`CONCAT(ROUND((((SUM(${summaryRevRegional.revAllM})/SUM(${rgbRegional.mtdRgb}))/(SUM(${summaryRevRegional.revAllM12})/SUM(${rgbRegional.rgbDataLastYear})))-1)*100,2),'%')`.as('yoy_arpu'),

                    mtd_arpu_data: sql<number>`ROUND(SUM(${summaryRevRegional.revBbM})/SUM(${rgbRegional.mtdRgbData}),0)`.as('mtd_arpu_data'),
                    mom_arpu_data: sql<string>`CONCAT(ROUND((((SUM(${summaryRevRegional.revBbM})/SUM(${rgbRegional.mtdRgbData}))/(SUM(${summaryRevRegional.revBbM1})/SUM(${rgbRegional.rgbDataLastMonth})))-1)*100,2),'%')`.as('mom_arpu_data'),
                    yoy_arpu_data: sql<string>`CONCAT(ROUND((((SUM(${summaryRevRegional.revBbM})/SUM(${rgbRegional.mtdRgbData}))/(SUM(${summaryRevRegional.revBbM12})/SUM(${rgbRegional.rgbDataLastYear})))-1)*100,2),'%')`.as('yoy_arpu_data'),

                    mtd_arpu_digital: sql<number>`ROUND(SUM(${summaryRevRegional.revDigitalM})/SUM(${rgbRegional.mtdRgbDigital}),0)`.as('mtd_arpu_digital'),
                    mom_arpu_digital: sql<string>`CONCAT(ROUND((((SUM(${summaryRevRegional.revDigitalM})/SUM(${rgbRegional.mtdRgbDigital}))/(SUM(${summaryRevRegional.revDigitalM1})/SUM(${rgbRegional.rgbDigitalLastMonth})))-1)*100,2),'%')`.as('mom_arpu_digital'),
                    yoy_arpu_digital: sql<string>`CONCAT(ROUND((((SUM(${summaryRevRegional.revDigitalM})/SUM(${rgbRegional.mtdRgbDigital}))/(SUM(${summaryRevRegional.revDigitalM12})/SUM(${rgbRegional.rgbDigitalLastYear})))-1)*100,2),'%')`.as('yoy_arpu_digital'),

                    mtd_rpmb: sql<number>`ROUND(SUM(${summaryRevRegional.revAllM})/(SUM(${rgbRegional.mtdPayload})/1024),2)`.as('mtd_rpmb'),
                    mom_rpmb: sql<string>`CONCAT(ROUND(((SUM(${summaryRevRegional.revAllM})/(SUM(${rgbRegional.mtdPayload})/1024)) / (SUM(${summaryRevRegional.revAllM1})/(SUM(${rgbRegional.payloadLastMonth})/1024))-1)*100,2),'%')`.as('mom_rpmb'),
                    yoy_rpmb: sql<string>`CONCAT(ROUND(((SUM(${summaryRevRegional.revAllM})/(SUM(${rgbRegional.mtdPayload})/1024)) / (SUM(${summaryRevRegional.revAllM12})/(SUM(${rgbRegional.payloadLastYear})/1024))-1)*100,2),'%')`.as('yoy_rpmb'),
                })
                .from(regionalTerritory)
                .leftJoin(rgbRegional, eq(regionalTerritory.regional, rgbRegional.regional))
                .leftJoin(regionalTargetRevenue, eq(regionalTerritory.regional, regionalTargetRevenue.regional))
                .leftJoin(summaryRevRegional, eq(regionalTerritory.regional, summaryRevRegional.regional))
                .groupBy(regionalTerritory.regional)

            const branchHeaderQuery = db
                .selectDistinct({
                    name: sql<string | null>`'BRANCH'`,
                    target_rgb_all: sql<number>`''`,
                    mtd_rgb: sql<number>`''`,
                    mom_rgb: sql<string>`''`,
                    yoy_rgb: sql<string>`''`,
                    ach_target_fm_rgb: sql<string>`''`,
                    drr_rgb: sql<string>`''`,
                    gap_to_target_rgb: sql<string>`''`,
                    penetration_to_cb: sql<string>`''`,

                    mtd_rgb_data: sql<number>`''`,
                    mom_rgb_data: sql<string>`''`,
                    yoy_rgb_data: sql<string>`''`,

                    mtd_rgb_digital: sql<number>`''`,
                    mom_rgb_digital: sql<string>`''`,
                    yoy_rgb_digital: sql<string>`''`,

                    mtd_arpu: sql<number>`''`,
                    mom_arpu: sql<string>`''`,
                    yoy_arpu: sql<string>`''`,

                    mtd_arpu_data: sql<number>`''`,
                    mom_arpu_data: sql<string>`''`,
                    yoy_arpu_data: sql<string>`''`,

                    mtd_arpu_digital: sql<number>`''`,
                    mom_arpu_digital: sql<string>`''`,
                    yoy_arpu_digital: sql<string>`''`,

                    mtd_rpmb: sql<number>`''`,
                    mom_rpmb: sql<string>`''`,
                    yoy_rpmb: sql<string>`''`,
                })
                .from(feiTargetPuma)

            const branchTerritory = db
                .select({
                    branch: territoryArea4.branch,
                })
                .from(territoryArea4)
                .where(and(
                    eq(territoryArea4.regional, 'PUMA'),
                    branch ? eq(territoryArea4.branch, branch) : undefined
                ))
                .groupBy(territoryArea4.branch)
                .as('a')

            const rgbBranch = db
                .select({
                    branch: summaryRgbHqBranch.branch,
                    cbLastYear: summaryRgbHqBranch.cb_m12,
                    cbLastMonth: summaryRgbHqBranch.cb_m1,
                    mtdCb: summaryRgbHqBranch.cb_mtd,
                    rgbLastYear: summaryRgbHqBranch.rgb_m12,
                    rgbLastMonth: summaryRgbHqBranch.rgb_m1,
                    mtdRgb: summaryRgbHqBranch.rgb_mtd,
                    rgbDataLastYear: summaryRgbHqBranch.rgb_data_m12,
                    rgbDataLastMonth: summaryRgbHqBranch.rgb_data_m1,
                    mtdRgbData: summaryRgbHqBranch.rgb_data_mtd,
                    rgbDigitalLastYear: summaryRgbHqBranch.rgb_digital_m12,
                    rgbDigitalLastMonth: summaryRgbHqBranch.rgb_digital_m1,
                    mtdRgbDigital: summaryRgbHqBranch.rgb_digital_mtd,
                    payloadLastYear: summaryRgbHqBranch.payload_user_m12,
                    payloadLastMonth: summaryRgbHqBranch.payload_user_m1,
                    mtdPayload: summaryRgbHqBranch.payload_user_mtd
                })
                .from(summaryRgbHqBranch)
                .where(and(
                    eq(summaryRgbHqBranch.event_date, currDate),
                    eq(summaryRgbHqBranch.regional, 'PUMA'),
                    eq(summaryRgbHqBranch.catlos, '1.0-1 mo'),
                    eq(summaryRgbHqBranch.brand, 'all')
                ))
                .groupBy(summaryRgbHqBranch.branch)
                .as('b')

            const branchTargetRevenue = db
                .select({
                    branch: territoryArea4.branch,
                    target_paying_los_0_1: sum(feiTargetPuma.paying_los_0_1).as('target_paying_los_0_1')
                })
                .from(feiTargetPuma)
                .rightJoin(territoryArea4, eq(feiTargetPuma.territory, territoryArea4.kabupaten))
                .where(eq(feiTargetPuma.periode, yyyyMM))
                .groupBy(territoryArea4.branch)
                .as('c')

            const summaryRevBranch = db
                .select({
                    branch: summaryRevAllBranch.branch,
                    revAllM: summaryRevAllBranch.rev_all_m,
                    revAllM1: sql<number>`SUM(CASE WHEN ${summaryRevAllBranch.tgl} = ${prevDate} THEN ${summaryRevAllBranch.rev_all_m} END)`.as('rev_all_m1'),
                    revAllM12: sql<number>`SUM(CASE WHEN ${summaryRevAllBranch.tgl} = ${prevYearCurrDate} THEN ${summaryRevAllBranch.rev_all_m} END)`.as('rev_all_m12'),

                    revBbM: summaryRevAllBranch.rev_bb_m,
                    revBbM1: sql<number>`SUM(CASE WHEN ${summaryRevAllBranch.tgl} = ${prevDate} THEN ${summaryRevAllBranch.rev_bb_m} END)`.as('rev_bb_m1'),
                    revBbM12: sql<number>`SUM(CASE WHEN ${summaryRevAllBranch.tgl} = ${prevYearCurrDate} THEN ${summaryRevAllBranch.rev_bb_m} END)`.as('rev_bb_m12'),

                    revVoiceM: summaryRevAllBranch.rev_voice_m,
                    revVoiceM1: sql<number>`SUM(CASE WHEN ${summaryRevAllBranch.tgl} = ${prevDate} THEN ${summaryRevAllBranch.rev_voice_m} END)`.as('rev_voice_m1'),
                    revVoiceM12: sql<number>`SUM(CASE WHEN ${summaryRevAllBranch.tgl} = ${prevYearCurrDate} THEN ${summaryRevAllBranch.rev_voice_m} END)`.as('rev_voice_m12'),

                    revDigitalM: summaryRevAllBranch.rev_digital_m,
                    revDigitalM1: sql<number>`SUM(CASE WHEN ${summaryRevAllBranch.tgl} = ${prevDate} THEN ${summaryRevAllBranch.rev_digital_m} END)`.as('rev_digital_m1'),
                    revDigitalM12: sql<number>`SUM(CASE WHEN ${summaryRevAllBranch.tgl} = ${prevYearCurrDate} THEN ${summaryRevAllBranch.rev_digital_m} END)`.as('rev_digital_m12')
                })
                .from(summaryRevAllBranch)
                .where(and(
                    eq(summaryRevAllBranch.regional, 'PUMA'),
                    eq(summaryRevAllBranch.newAbcStrate, 'all')
                ))
                .groupBy(summaryRevAllBranch.branch)
                .as('d')

            const revenueBranch = db
                .select({
                    name: branchTerritory.branch,
                    target_rgb_all: sql<number>`ROUND(SUM(${branchTargetRevenue.target_paying_los_0_1}),0)`.as('target_rgb_all'),
                    mtd_rgb: sql<number>`ROUND(SUM(${rgbBranch.mtdRgb}),0)`.as('mtd_rgb'),
                    mom_rgb: sql<string>`CONCAT(ROUND(((SUM(${rgbBranch.mtdRgb})/SUM(${rgbBranch.rgbLastMonth}))-1)*100,2),'%')`.as('mom_rgb'),
                    yoy_rgb: sql<string>`CONCAT(ROUND(((SUM(${rgbBranch.mtdRgb})/SUM(${rgbBranch.rgbLastYear}))-1)*100,2),'%')`.as('yoy_rgb'),

                    ach_target_fm_rgb: sql<string>`CONCAT(ROUND((SUM(${rgbBranch.mtdRgb})/SUM(${branchTargetRevenue.target_paying_los_0_1}))*100,2),'%')`.as('ach_target_fm_rgb'),
                    drr_rgb: sql<string>`CONCAT(ROUND((SUM(${rgbBranch.mtdRgb})/(${today}/${daysInMonth}*(SUM(${branchTargetRevenue.target_paying_los_0_1}))))*100,2),'%')`.as('drr_rgb'),
                    gap_to_target_rgb: sql<string>`ROUND((COALESCE(SUM(${rgbBranch.mtdRgb}) - SUM(${branchTargetRevenue.target_paying_los_0_1}),0)),0)`.as('gap_to_target_rgb'),
                    penetration_to_cb: sql<string>`CONCAT(ROUND((SUM(${rgbBranch.mtdRgb})/SUM(${rgbBranch.mtdCb})*100),2),'%')`.as('penetration_to_cb'),

                    mtd_rgb_data: sql<number>`ROUND(SUM(${rgbBranch.mtdRgbData}),0)`.as('mtd_rgb_data'),
                    mom_rgb_data: sql<string>`CONCAT(ROUND((((SUM(${rgbBranch.mtdRgbData})/SUM(${rgbBranch.rgbDataLastMonth}))-1)*100),2),'%')`.as('mom_rgb_data'),
                    yoy_rgb_data: sql<string>`CONCAT(ROUND((((SUM(${rgbBranch.mtdRgbData})/SUM(${rgbBranch.rgbDataLastYear}))-1)*100),2),'%')`.as('yoy_rgb_data'),

                    mtd_rgb_digital: sql<number>`ROUND(SUM(${rgbBranch.mtdRgbDigital}),0)`.as('mtd_rgb_digital'),
                    mom_rgb_digital: sql<string>`CONCAT(ROUND((((SUM(${rgbBranch.mtdRgbDigital})/SUM(${rgbBranch.rgbDigitalLastMonth}))-1)*100),2),'%')`.as('mom_rgb_digital'),
                    yoy_rgb_digital: sql<string>`CONCAT(ROUND((((SUM(${rgbBranch.mtdRgbDigital})/SUM(${rgbBranch.rgbDigitalLastYear}))-1)*100),2),'%')`.as('yoy_rgb_digital'),

                    mtd_arpu: sql<number>`ROUND(SUM(${summaryRevBranch.revAllM})/SUM(${rgbBranch.mtdRgb}),0)`.as('mtd_arpu'),
                    mom_arpu: sql<string>`CONCAT(ROUND((((SUM(${summaryRevBranch.revAllM})/SUM(${rgbBranch.mtdRgb}))/(SUM(${summaryRevBranch.revAllM1})/SUM(${rgbBranch.rgbDataLastMonth})))-1)*100,2),'%')`.as('mom_arpu'),
                    yoy_arpu: sql<string>`CONCAT(ROUND((((SUM(${summaryRevBranch.revAllM})/SUM(${rgbBranch.mtdRgb}))/(SUM(${summaryRevBranch.revAllM12})/SUM(${rgbBranch.rgbDataLastYear})))-1)*100,2),'%')`.as('yoy_arpu'),

                    mtd_arpu_data: sql<number>`ROUND(SUM(${summaryRevBranch.revBbM})/SUM(${rgbBranch.mtdRgbData}),0)`.as('mtd_arpu_data'),
                    mom_arpu_data: sql<string>`CONCAT(ROUND((((SUM(${summaryRevBranch.revBbM})/SUM(${rgbBranch.mtdRgbData}))/(SUM(${summaryRevBranch.revBbM1})/SUM(${rgbBranch.rgbDataLastMonth})))-1)*100,2),'%')`.as('mom_arpu_data'),
                    yoy_arpu_data: sql<string>`CONCAT(ROUND((((SUM(${summaryRevBranch.revBbM})/SUM(${rgbBranch.mtdRgbData}))/(SUM(${summaryRevBranch.revBbM12})/SUM(${rgbBranch.rgbDataLastYear})))-1)*100,2),'%')`.as('yoy_arpu_data'),

                    mtd_arpu_digital: sql<number>`ROUND(SUM(${summaryRevBranch.revDigitalM})/SUM(${rgbBranch.mtdRgbDigital}),0)`.as('mtd_arpu_digital'),
                    mom_arpu_digital: sql<string>`CONCAT(ROUND((((SUM(${summaryRevBranch.revDigitalM})/SUM(${rgbBranch.mtdRgbDigital}))/(SUM(${summaryRevBranch.revDigitalM1})/SUM(${rgbBranch.rgbDigitalLastMonth})))-1)*100,2),'%')`.as('mom_arpu_digital'),
                    yoy_arpu_digital: sql<string>`CONCAT(ROUND((((SUM(${summaryRevBranch.revDigitalM})/SUM(${rgbBranch.mtdRgbDigital}))/(SUM(${summaryRevBranch.revDigitalM12})/SUM(${rgbBranch.rgbDigitalLastYear})))-1)*100,2),'%')`.as('yoy_arpu_digital'),

                    mtd_rpmb: sql<number>`ROUND(SUM(${summaryRevBranch.revAllM})/(SUM(${rgbBranch.mtdPayload})/1024),2)`.as('mtd_rpmb'),
                    mom_rpmb: sql<string>`CONCAT(ROUND(((SUM(${summaryRevBranch.revAllM})/(SUM(${rgbBranch.mtdPayload})/1024)) / (SUM(${summaryRevBranch.revAllM1})/(SUM(${rgbBranch.payloadLastMonth})/1024))-1)*100,2),'%')`.as('mom_rpmb'),
                    yoy_rpmb: sql<string>`CONCAT(ROUND(((SUM(${summaryRevBranch.revAllM})/(SUM(${rgbBranch.mtdPayload})/1024)) / (SUM(${summaryRevBranch.revAllM12})/(SUM(${rgbBranch.payloadLastYear})/1024))-1)*100,2),'%')`.as('yoy_rpmb'),
                })
                .from(branchTerritory)
                .leftJoin(rgbBranch, eq(branchTerritory.branch, rgbBranch.branch))
                .leftJoin(branchTargetRevenue, eq(branchTerritory.branch, branchTargetRevenue.branch))
                .leftJoin(summaryRevBranch, eq(branchTerritory.branch, summaryRevBranch.branch))
                .groupBy(branchTerritory.branch)

            const subbranchHeaderQuery = db
                .selectDistinct({
                    name: sql<string | null>`'SUBBRANCH'`,
                    target_rgb_all: sql<number>`''`,
                    mtd_rgb: sql<number>`''`,
                    mom_rgb: sql<string>`''`,
                    yoy_rgb: sql<string>`''`,
                    ach_target_fm_rgb: sql<string>`''`,
                    drr_rgb: sql<string>`''`,
                    gap_to_target_rgb: sql<string>`''`,
                    penetration_to_cb: sql<string>`''`,

                    mtd_rgb_data: sql<number>`''`,
                    mom_rgb_data: sql<string>`''`,
                    yoy_rgb_data: sql<string>`''`,

                    mtd_rgb_digital: sql<number>`''`,
                    mom_rgb_digital: sql<string>`''`,
                    yoy_rgb_digital: sql<string>`''`,

                    mtd_arpu: sql<number>`''`,
                    mom_arpu: sql<string>`''`,
                    yoy_arpu: sql<string>`''`,

                    mtd_arpu_data: sql<number>`''`,
                    mom_arpu_data: sql<string>`''`,
                    yoy_arpu_data: sql<string>`''`,

                    mtd_arpu_digital: sql<number>`''`,
                    mom_arpu_digital: sql<string>`''`,
                    yoy_arpu_digital: sql<string>`''`,

                    mtd_rpmb: sql<number>`''`,
                    mom_rpmb: sql<string>`''`,
                    yoy_rpmb: sql<string>`''`,
                })
                .from(feiTargetPuma)

            const subbranchTerritory = db
                .select({
                    subbranch: territoryArea4.subbranch,
                })
                .from(territoryArea4)
                .where(branch && subbranch ? and(
                    eq(territoryArea4.regional, 'PUMA'),
                    eq(territoryArea4.branch, branch),
                    eq(territoryArea4.subbranch, subbranch)
                ) : branch ? and(
                    eq(territoryArea4.regional, 'PUMA'),
                    eq(territoryArea4.branch, branch)
                ) : eq(territoryArea4.regional, 'PUMA'))
                .groupBy(territoryArea4.subbranch)
                .as('a')

            const rgbSubbranch = db
                .select({
                    subbranch: sql<string>`
                    CASE
                        WHEN kabupaten IN ('AMBON','KOTA AMBON','MALUKU TENGAH','SERAM BAGIAN TIMUR') THEN 'AMBON'
                        WHEN kabupaten IN ('KEPULAUAN ARU','KOTA TUAL','MALUKU BARAT DAYA','MALUKU TENGGARA','MALUKU TENGGARA BARAT','KEPULAUAN TANIMBAR') THEN 'KEPULAUAN AMBON'
                        WHEN kabupaten IN ('BURU', 'BURU SELATAN', 'SERAM BAGIAN BARAT') THEN 'SERAM BURU'
                        WHEN kabupaten IN ('KOTA JAYAPURA') THEN 'JAYAPURA'
                        WHEN kabupaten IN ('JAYAPURA','KEEROM','MAMBERAMO RAYA','SARMI','BIAK','BIAK NUMFOR','KEPULAUAN YAPEN','SUPIORI','WAROPEN','JAYAWIJAYA','LANNY JAYA','MAMBERAMO TENGAH','NDUGA','PEGUNUNGAN BINTANG','TOLIKARA','YAHUKIMO','YALIMO') THEN 'SENTANI'
                        WHEN kabupaten IN ('MANOKWARI') THEN 'MANOKWARI'
                        WHEN kabupaten IN ('FAKFAK','FAK FAK','KAIMANA','MANOKWARI SELATAN','PEGUNUNGAN ARFAK','TELUK BINTUNI','TELUK WONDAMA') THEN 'MANOKWARI OUTER'
                        WHEN kabupaten IN ('KOTA SORONG','MAYBRAT','RAJA AMPAT','SORONG','SORONG SELATAN','TAMBRAUW') THEN 'SORONG RAJA AMPAT'
                        WHEN kabupaten IN ('ASMAT', 'BOVEN DIGOEL', 'MAPPI', 'MERAUKE') THEN 'MERAUKE'
                        WHEN kabupaten IN ('INTAN JAYA','MIMIKA','PUNCAK','PUNCAK JAYA','TIMIKA') THEN 'MIMIKA'
                        WHEN kabupaten IN ('DEIYAI', 'DOGIYAI', 'NABIRE', 'PANIAI') THEN 'NABIRE'
                        ELSE NULL
                    END
                    `.as('subbranch'),
                    cbLastYear: summaryRgbHqKabupaten.cb_m12,
                    cbLastMonth: summaryRgbHqKabupaten.cb_m1,
                    mtdCb: summaryRgbHqKabupaten.cb_mtd,
                    rgbLastYear: summaryRgbHqKabupaten.rgb_m12,
                    rgbLastMonth: summaryRgbHqKabupaten.rgb_m1,
                    mtdRgb: summaryRgbHqKabupaten.rgb_mtd,
                    rgbDataLastYear: summaryRgbHqKabupaten.rgb_data_m12,
                    rgbDataLastMonth: summaryRgbHqKabupaten.rgb_data_m1,
                    mtdRgbData: summaryRgbHqKabupaten.rgb_data_mtd,
                    rgbDigitalLastYear: summaryRgbHqKabupaten.rgb_digital_m12,
                    rgbDigitalLastMonth: summaryRgbHqKabupaten.rgb_digital_m1,
                    mtdRgbDigital: summaryRgbHqKabupaten.rgb_digital_mtd,
                    payloadLastYear: summaryRgbHqKabupaten.payload_user_m12,
                    payloadLastMonth: summaryRgbHqKabupaten.payload_user_m1,
                    mtdPayload: summaryRgbHqKabupaten.payload_user_mtd
                })
                .from(summaryRgbHqKabupaten)
                .where(and(
                    eq(summaryRgbHqKabupaten.event_date, currDate),
                    eq(summaryRgbHqKabupaten.regional, 'PUMA'),
                    eq(summaryRgbHqKabupaten.catlos, '1.0-1 mo'),
                    eq(summaryRgbHqKabupaten.brand, 'all')
                ))
                .groupBy(sql`1`)
                .as('b')

            const subbranchTargetRevenue = db
                .select({
                    subbranch: territoryArea4.subbranch,
                    target_paying_los_0_1: sum(feiTargetPuma.paying_los_0_1).as('target_paying_los_0_1')
                })
                .from(feiTargetPuma)
                .rightJoin(territoryArea4, eq(feiTargetPuma.territory, territoryArea4.kabupaten))
                .where(eq(feiTargetPuma.periode, yyyyMM))
                .groupBy(territoryArea4.subbranch)
                .as('c')

            const summaryRevSubbranch = db
                .select({
                    subbranch: summaryRevAllSubbranch.subbranch,
                    revAllM: summaryRevAllSubbranch.rev_all_m,
                    revAllM1: sql<number>`SUM(CASE WHEN ${summaryRevAllSubbranch.tgl} = ${prevDate} THEN ${summaryRevAllSubbranch.rev_all_m} END)`.as('rev_all_m1'),
                    revAllM12: sql<number>`SUM(CASE WHEN ${summaryRevAllSubbranch.tgl} = ${prevYearCurrDate} THEN ${summaryRevAllSubbranch.rev_all_m} END)`.as('rev_all_m12'),

                    revBbM: summaryRevAllSubbranch.rev_bb_m,
                    revBbM1: sql<number>`SUM(CASE WHEN ${summaryRevAllSubbranch.tgl} = ${prevDate} THEN ${summaryRevAllSubbranch.rev_bb_m} END)`.as('rev_bb_m1'),
                    revBbM12: sql<number>`SUM(CASE WHEN ${summaryRevAllSubbranch.tgl} = ${prevYearCurrDate} THEN ${summaryRevAllSubbranch.rev_bb_m} END)`.as('rev_bb_m12'),

                    revVoiceM: summaryRevAllSubbranch.rev_voice_m,
                    revVoiceM1: sql<number>`SUM(CASE WHEN ${summaryRevAllSubbranch.tgl} = ${prevDate} THEN ${summaryRevAllSubbranch.rev_voice_m} END)`.as('rev_voice_m1'),
                    revVoiceM12: sql<number>`SUM(CASE WHEN ${summaryRevAllSubbranch.tgl} = ${prevYearCurrDate} THEN ${summaryRevAllSubbranch.rev_voice_m} END)`.as('rev_voice_m12'),

                    revDigitalM: summaryRevAllSubbranch.rev_digital_m,
                    revDigitalM1: sql<number>`SUM(CASE WHEN ${summaryRevAllSubbranch.tgl} = ${prevDate} THEN ${summaryRevAllSubbranch.rev_digital_m} END)`.as('rev_digital_m1'),
                    revDigitalM12: sql<number>`SUM(CASE WHEN ${summaryRevAllSubbranch.tgl} = ${prevYearCurrDate} THEN ${summaryRevAllSubbranch.rev_digital_m} END)`.as('rev_digital_m12')
                })
                .from(summaryRevAllSubbranch)
                .where(and(
                    eq(summaryRevAllSubbranch.regional, 'PUMA'),
                    eq(summaryRevAllSubbranch.newAbcStrate, 'all')
                ))
                .groupBy(summaryRevAllSubbranch.subbranch)
                .as('d')

            const revenueSubbranch = db
                .select({
                    name: subbranchTerritory.subbranch,
                    target_rgb_all: sql<number>`ROUND(SUM(${subbranchTargetRevenue.target_paying_los_0_1}),0)`.as('target_rgb_all'),
                    mtd_rgb: sql<number>`ROUND(SUM(${rgbSubbranch.mtdRgb}),0)`.as('mtd_rgb'),
                    mom_rgb: sql<string>`CONCAT(ROUND(((SUM(${rgbSubbranch.mtdRgb})/SUM(${rgbSubbranch.rgbLastMonth}))-1)*100,2),'%')`.as('mom_rgb'),
                    yoy_rgb: sql<string>`CONCAT(ROUND(((SUM(${rgbSubbranch.mtdRgb})/SUM(${rgbSubbranch.rgbLastYear}))-1)*100,2),'%')`.as('yoy_rgb'),

                    ach_target_fm_rgb: sql<string>`CONCAT(ROUND((SUM(${rgbSubbranch.mtdRgb})/SUM(${subbranchTargetRevenue.target_paying_los_0_1}))*100,2),'%')`.as('ach_target_fm_rgb'),
                    drr_rgb: sql<string>`CONCAT(ROUND((SUM(${rgbSubbranch.mtdRgb})/(${today}/${daysInMonth}*(SUM(${subbranchTargetRevenue.target_paying_los_0_1}))))*100,2),'%')`.as('drr_rgb'),
                    gap_to_target_rgb: sql<string>`ROUND((COALESCE(SUM(${rgbSubbranch.mtdRgb}) - SUM(${subbranchTargetRevenue.target_paying_los_0_1}),0)),0)`.as('gap_to_target_rgb'),
                    penetration_to_cb: sql<string>`CONCAT(ROUND((SUM(${rgbSubbranch.mtdRgb})/SUM(${rgbSubbranch.mtdCb})*100),2),'%')`.as('penetration_to_cb'),

                    mtd_rgb_data: sql<number>`ROUND(SUM(${rgbSubbranch.mtdRgbData}),0)`.as('mtd_rgb_data'),
                    mom_rgb_data: sql<string>`CONCAT(ROUND((((SUM(${rgbSubbranch.mtdRgbData})/SUM(${rgbSubbranch.rgbDataLastMonth}))-1)*100),2),'%')`.as('mom_rgb_data'),
                    yoy_rgb_data: sql<string>`CONCAT(ROUND((((SUM(${rgbSubbranch.mtdRgbData})/SUM(${rgbSubbranch.rgbDataLastYear}))-1)*100),2),'%')`.as('yoy_rgb_data'),

                    mtd_rgb_digital: sql<number>`ROUND(SUM(${rgbSubbranch.mtdRgbDigital}),0)`.as('mtd_rgb_digital'),
                    mom_rgb_digital: sql<string>`CONCAT(ROUND((((SUM(${rgbSubbranch.mtdRgbDigital})/SUM(${rgbSubbranch.rgbDigitalLastMonth}))-1)*100),2),'%')`.as('mom_rgb_digital'),
                    yoy_rgb_digital: sql<string>`CONCAT(ROUND((((SUM(${rgbSubbranch.mtdRgbDigital})/SUM(${rgbSubbranch.rgbDigitalLastYear}))-1)*100),2),'%')`.as('yoy_rgb_digital'),

                    mtd_arpu: sql<number>`ROUND(SUM(${summaryRevSubbranch.revAllM})/SUM(${rgbSubbranch.mtdRgb}),0)`.as('mtd_arpu'),
                    mom_arpu: sql<string>`CONCAT(ROUND((((SUM(${summaryRevSubbranch.revAllM})/SUM(${rgbSubbranch.mtdRgb}))/(SUM(${summaryRevSubbranch.revAllM1})/SUM(${rgbSubbranch.rgbDataLastMonth})))-1)*100,2),'%')`.as('mom_arpu'),
                    yoy_arpu: sql<string>`CONCAT(ROUND((((SUM(${summaryRevSubbranch.revAllM})/SUM(${rgbSubbranch.mtdRgb}))/(SUM(${summaryRevSubbranch.revAllM12})/SUM(${rgbSubbranch.rgbDataLastYear})))-1)*100,2),'%')`.as('yoy_arpu'),

                    mtd_arpu_data: sql<number>`ROUND(SUM(${summaryRevSubbranch.revBbM})/SUM(${rgbSubbranch.mtdRgbData}),0)`.as('mtd_arpu_data'),
                    mom_arpu_data: sql<string>`CONCAT(ROUND((((SUM(${summaryRevSubbranch.revBbM})/SUM(${rgbSubbranch.mtdRgbData}))/(SUM(${summaryRevSubbranch.revBbM1})/SUM(${rgbSubbranch.rgbDataLastMonth})))-1)*100,2),'%')`.as('mom_arpu_data'),
                    yoy_arpu_data: sql<string>`CONCAT(ROUND((((SUM(${summaryRevSubbranch.revBbM})/SUM(${rgbSubbranch.mtdRgbData}))/(SUM(${summaryRevSubbranch.revBbM12})/SUM(${rgbSubbranch.rgbDataLastYear})))-1)*100,2),'%')`.as('yoy_arpu_data'),

                    mtd_arpu_digital: sql<number>`ROUND(SUM(${summaryRevSubbranch.revDigitalM})/SUM(${rgbSubbranch.mtdRgbDigital}),0)`.as('mtd_arpu_digital'),
                    mom_arpu_digital: sql<string>`CONCAT(ROUND((((SUM(${summaryRevSubbranch.revDigitalM})/SUM(${rgbSubbranch.mtdRgbDigital}))/(SUM(${summaryRevSubbranch.revDigitalM1})/SUM(${rgbSubbranch.rgbDigitalLastMonth})))-1)*100,2),'%')`.as('mom_arpu_digital'),
                    yoy_arpu_digital: sql<string>`CONCAT(ROUND((((SUM(${summaryRevSubbranch.revDigitalM})/SUM(${rgbSubbranch.mtdRgbDigital}))/(SUM(${summaryRevSubbranch.revDigitalM12})/SUM(${rgbSubbranch.rgbDigitalLastYear})))-1)*100,2),'%')`.as('yoy_arpu_digital'),

                    mtd_rpmb: sql<number>`ROUND(SUM(${summaryRevSubbranch.revAllM})/(SUM(${rgbSubbranch.mtdPayload})/1024),2)`.as('mtd_rpmb'),
                    mom_rpmb: sql<string>`CONCAT(ROUND(((SUM(${summaryRevSubbranch.revAllM})/(SUM(${rgbSubbranch.mtdPayload})/1024)) / (SUM(${summaryRevSubbranch.revAllM1})/(SUM(${rgbSubbranch.payloadLastMonth})/1024))-1)*100,2),'%')`.as('mom_rpmb'),
                    yoy_rpmb: sql<string>`CONCAT(ROUND(((SUM(${summaryRevSubbranch.revAllM})/(SUM(${rgbSubbranch.mtdPayload})/1024)) / (SUM(${summaryRevSubbranch.revAllM12})/(SUM(${rgbSubbranch.payloadLastYear})/1024))-1)*100,2),'%')`.as('yoy_rpmb'),
                })
                .from(subbranchTerritory)
                .leftJoin(rgbSubbranch, eq(subbranchTerritory.subbranch, sql`b.subbranch`))
                .leftJoin(subbranchTargetRevenue, eq(subbranchTerritory.subbranch, subbranchTargetRevenue.subbranch))
                .leftJoin(summaryRevSubbranch, eq(subbranchTerritory.subbranch, summaryRevSubbranch.subbranch))
                .groupBy(subbranchTerritory.subbranch)

            const clusterHeaderQuery = db
                .selectDistinct({
                    name: sql<string | null>`'CLUSTER'`,
                    target_rgb_all: sql<number>`''`,
                    mtd_rgb: sql<number>`''`,
                    mom_rgb: sql<string>`''`,
                    yoy_rgb: sql<string>`''`,
                    ach_target_fm_rgb: sql<string>`''`,
                    drr_rgb: sql<string>`''`,
                    gap_to_target_rgb: sql<string>`''`,
                    penetration_to_cb: sql<string>`''`,

                    mtd_rgb_data: sql<number>`''`,
                    mom_rgb_data: sql<string>`''`,
                    yoy_rgb_data: sql<string>`''`,

                    mtd_rgb_digital: sql<number>`''`,
                    mom_rgb_digital: sql<string>`''`,
                    yoy_rgb_digital: sql<string>`''`,

                    mtd_arpu: sql<number>`''`,
                    mom_arpu: sql<string>`''`,
                    yoy_arpu: sql<string>`''`,

                    mtd_arpu_data: sql<number>`''`,
                    mom_arpu_data: sql<string>`''`,
                    yoy_arpu_data: sql<string>`''`,

                    mtd_arpu_digital: sql<number>`''`,
                    mom_arpu_digital: sql<string>`''`,
                    yoy_arpu_digital: sql<string>`''`,

                    mtd_rpmb: sql<number>`''`,
                    mom_rpmb: sql<string>`''`,
                    yoy_rpmb: sql<string>`''`,
                })
                .from(feiTargetPuma)

            const clusterTerritory = db
                .select({
                    cluster: territoryArea4.cluster,
                })
                .from(territoryArea4)
                .where(branch && subbranch && cluster ? and(
                    eq(territoryArea4.regional, 'PUMA'),
                    eq(territoryArea4.branch, branch),
                    eq(territoryArea4.subbranch, subbranch),
                    eq(territoryArea4.cluster, cluster),
                ) : branch && subbranch ? and(
                    eq(territoryArea4.regional, 'PUMA'),
                    eq(territoryArea4.branch, branch),
                    eq(territoryArea4.subbranch, subbranch),
                ) : branch ? and(
                    eq(territoryArea4.regional, 'PUMA'),
                    eq(territoryArea4.branch, branch),
                ) : eq(territoryArea4.regional, 'PUMA'))
                .groupBy(territoryArea4.cluster)
                .as('a')

            const rgbCluster = db
                .select({
                    cluster: summaryRgbHqCluster.cluster,
                    cbLastYear: summaryRgbHqCluster.cb_m12,
                    cbLastMonth: summaryRgbHqCluster.cb_m1,
                    mtdCb: summaryRgbHqCluster.cb_mtd,
                    rgbLastYear: summaryRgbHqCluster.rgb_m12,
                    rgbLastMonth: summaryRgbHqCluster.rgb_m1,
                    mtdRgb: summaryRgbHqCluster.rgb_mtd,
                    rgbDataLastYear: summaryRgbHqCluster.rgb_data_m12,
                    rgbDataLastMonth: summaryRgbHqCluster.rgb_data_m1,
                    mtdRgbData: summaryRgbHqCluster.rgb_data_mtd,
                    rgbDigitalLastYear: summaryRgbHqCluster.rgb_digital_m12,
                    rgbDigitalLastMonth: summaryRgbHqCluster.rgb_digital_m1,
                    mtdRgbDigital: summaryRgbHqCluster.rgb_digital_mtd,
                    payloadLastYear: summaryRgbHqCluster.payload_user_m12,
                    payloadLastMonth: summaryRgbHqCluster.payload_user_m1,
                    mtdPayload: summaryRgbHqCluster.payload_user_mtd
                })
                .from(summaryRgbHqCluster)
                .where(and(
                    eq(summaryRgbHqCluster.event_date, currDate),
                    eq(summaryRgbHqCluster.regional, 'PUMA'),
                    eq(summaryRgbHqCluster.catlos, '1.0-1 mo'),
                    eq(summaryRgbHqCluster.brand, 'all')
                ))
                .groupBy(summaryRgbHqCluster.cluster)
                .as('b')

            const clusterTargetRevenue = db
                .select({
                    cluster: territoryArea4.cluster,
                    target_paying_los_0_1: sum(feiTargetPuma.paying_los_0_1).as('target_paying_los_0_1')
                })
                .from(feiTargetPuma)
                .rightJoin(territoryArea4, eq(feiTargetPuma.territory, territoryArea4.kabupaten))
                .where(eq(feiTargetPuma.periode, yyyyMM))
                .groupBy(territoryArea4.cluster)
                .as('c')

            const summaryRevCluster = db
                .select({
                    cluster: summaryRevAllCluster.cluster,
                    revAllM: summaryRevAllCluster.rev_all_m,
                    revAllM1: sql<number>`SUM(CASE WHEN ${summaryRevAllCluster.tgl} = ${prevDate} THEN ${summaryRevAllCluster.rev_all_m} END)`.as('rev_all_m1'),
                    revAllM12: sql<number>`SUM(CASE WHEN ${summaryRevAllCluster.tgl} = ${prevYearCurrDate} THEN ${summaryRevAllCluster.rev_all_m} END)`.as('rev_all_m12'),

                    revBbM: summaryRevAllCluster.rev_bb_m,
                    revBbM1: sql<number>`SUM(CASE WHEN ${summaryRevAllCluster.tgl} = ${prevDate} THEN ${summaryRevAllCluster.rev_bb_m} END)`.as('rev_bb_m1'),
                    revBbM12: sql<number>`SUM(CASE WHEN ${summaryRevAllCluster.tgl} = ${prevYearCurrDate} THEN ${summaryRevAllCluster.rev_bb_m} END)`.as('rev_bb_m12'),

                    revVoiceM: summaryRevAllCluster.rev_voice_m,
                    revVoiceM1: sql<number>`SUM(CASE WHEN ${summaryRevAllCluster.tgl} = ${prevDate} THEN ${summaryRevAllCluster.rev_voice_m} END)`.as('rev_voice_m1'),
                    revVoiceM12: sql<number>`SUM(CASE WHEN ${summaryRevAllCluster.tgl} = ${prevYearCurrDate} THEN ${summaryRevAllCluster.rev_voice_m} END)`.as('rev_voice_m12'),

                    revDigitalM: summaryRevAllCluster.rev_digital_m,
                    revDigitalM1: sql<number>`SUM(CASE WHEN ${summaryRevAllCluster.tgl} = ${prevDate} THEN ${summaryRevAllCluster.rev_digital_m} END)`.as('rev_digital_m1'),
                    revDigitalM12: sql<number>`SUM(CASE WHEN ${summaryRevAllCluster.tgl} = ${prevYearCurrDate} THEN ${summaryRevAllCluster.rev_digital_m} END)`.as('rev_digital_m12')
                })
                .from(summaryRevAllCluster)
                .where(and(
                    eq(summaryRevAllCluster.regional, 'PUMA'),
                    eq(summaryRevAllCluster.newAbcStrate, 'all')
                ))
                .groupBy(summaryRevAllCluster.cluster)
                .as('d')

            const revenueCluster = db
                .select({
                    name: clusterTerritory.cluster,
                    target_rgb_all: sql<number>`ROUND(SUM(${clusterTargetRevenue.target_paying_los_0_1}),0)`.as('target_rgb_all'),
                    mtd_rgb: sql<number>`ROUND(SUM(${rgbCluster.mtdRgb}),0)`.as('mtd_rgb'),
                    mom_rgb: sql<string>`CONCAT(ROUND(((SUM(${rgbCluster.mtdRgb})/SUM(${rgbCluster.rgbLastMonth}))-1)*100,2),'%')`.as('mom_rgb'),
                    yoy_rgb: sql<string>`CONCAT(ROUND(((SUM(${rgbCluster.mtdRgb})/SUM(${rgbCluster.rgbLastYear}))-1)*100,2),'%')`.as('yoy_rgb'),

                    ach_target_fm_rgb: sql<string>`CONCAT(ROUND((SUM(${rgbCluster.mtdRgb})/SUM(${clusterTargetRevenue.target_paying_los_0_1}))*100,2),'%')`.as('ach_target_fm_rgb'),
                    drr_rgb: sql<string>`CONCAT(ROUND((SUM(${rgbCluster.mtdRgb})/(${today}/${daysInMonth}*(SUM(${clusterTargetRevenue.target_paying_los_0_1}))))*100,2),'%')`.as('drr_rgb'),
                    gap_to_target_rgb: sql<string>`ROUND((COALESCE(SUM(${rgbCluster.mtdRgb}) - SUM(${clusterTargetRevenue.target_paying_los_0_1}),0)),0)`.as('gap_to_target_rgb'),
                    penetration_to_cb: sql<string>`CONCAT(ROUND((SUM(${rgbCluster.mtdRgb})/SUM(${rgbCluster.mtdCb})*100),2),'%')`.as('penetration_to_cb'),

                    mtd_rgb_data: sql<number>`ROUND(SUM(${rgbCluster.mtdRgbData}),0)`.as('mtd_rgb_data'),
                    mom_rgb_data: sql<string>`CONCAT(ROUND((((SUM(${rgbCluster.mtdRgbData})/SUM(${rgbCluster.rgbDataLastMonth}))-1)*100),2),'%')`.as('mom_rgb_data'),
                    yoy_rgb_data: sql<string>`CONCAT(ROUND((((SUM(${rgbCluster.mtdRgbData})/SUM(${rgbCluster.rgbDataLastYear}))-1)*100),2),'%')`.as('yoy_rgb_data'),

                    mtd_rgb_digital: sql<number>`ROUND(SUM(${rgbCluster.mtdRgbDigital}),0)`.as('mtd_rgb_digital'),
                    mom_rgb_digital: sql<string>`CONCAT(ROUND((((SUM(${rgbCluster.mtdRgbDigital})/SUM(${rgbCluster.rgbDigitalLastMonth}))-1)*100),2),'%')`.as('mom_rgb_digital'),
                    yoy_rgb_digital: sql<string>`CONCAT(ROUND((((SUM(${rgbCluster.mtdRgbDigital})/SUM(${rgbCluster.rgbDigitalLastYear}))-1)*100),2),'%')`.as('yoy_rgb_digital'),

                    mtd_arpu: sql<number>`ROUND(SUM(${summaryRevCluster.revAllM})/SUM(${rgbCluster.mtdRgb}),0)`.as('mtd_arpu'),
                    mom_arpu: sql<string>`CONCAT(ROUND((((SUM(${summaryRevCluster.revAllM})/SUM(${rgbCluster.mtdRgb}))/(SUM(${summaryRevCluster.revAllM1})/SUM(${rgbCluster.rgbDataLastMonth})))-1)*100,2),'%')`.as('mom_arpu'),
                    yoy_arpu: sql<string>`CONCAT(ROUND((((SUM(${summaryRevCluster.revAllM})/SUM(${rgbCluster.mtdRgb}))/(SUM(${summaryRevCluster.revAllM12})/SUM(${rgbCluster.rgbDataLastYear})))-1)*100,2),'%')`.as('yoy_arpu'),

                    mtd_arpu_data: sql<number>`ROUND(SUM(${summaryRevCluster.revBbM})/SUM(${rgbCluster.mtdRgbData}),0)`.as('mtd_arpu_data'),
                    mom_arpu_data: sql<string>`CONCAT(ROUND((((SUM(${summaryRevCluster.revBbM})/SUM(${rgbCluster.mtdRgbData}))/(SUM(${summaryRevCluster.revBbM1})/SUM(${rgbCluster.rgbDataLastMonth})))-1)*100,2),'%')`.as('mom_arpu_data'),
                    yoy_arpu_data: sql<string>`CONCAT(ROUND((((SUM(${summaryRevCluster.revBbM})/SUM(${rgbCluster.mtdRgbData}))/(SUM(${summaryRevCluster.revBbM12})/SUM(${rgbCluster.rgbDataLastYear})))-1)*100,2),'%')`.as('yoy_arpu_data'),

                    mtd_arpu_digital: sql<number>`ROUND(SUM(${summaryRevCluster.revDigitalM})/SUM(${rgbCluster.mtdRgbDigital}),0)`.as('mtd_arpu_digital'),
                    mom_arpu_digital: sql<string>`CONCAT(ROUND((((SUM(${summaryRevCluster.revDigitalM})/SUM(${rgbCluster.mtdRgbDigital}))/(SUM(${summaryRevCluster.revDigitalM1})/SUM(${rgbCluster.rgbDigitalLastMonth})))-1)*100,2),'%')`.as('mom_arpu_digital'),
                    yoy_arpu_digital: sql<string>`CONCAT(ROUND((((SUM(${summaryRevCluster.revDigitalM})/SUM(${rgbCluster.mtdRgbDigital}))/(SUM(${summaryRevCluster.revDigitalM12})/SUM(${rgbCluster.rgbDigitalLastYear})))-1)*100,2),'%')`.as('yoy_arpu_digital'),

                    mtd_rpmb: sql<number>`ROUND(SUM(${summaryRevCluster.revAllM})/(SUM(${rgbCluster.mtdPayload})/1024),2)`.as('mtd_rpmb'),
                    mom_rpmb: sql<string>`CONCAT(ROUND(((SUM(${summaryRevCluster.revAllM})/(SUM(${rgbCluster.mtdPayload})/1024)) / (SUM(${summaryRevCluster.revAllM1})/(SUM(${rgbCluster.payloadLastMonth})/1024))-1)*100,2),'%')`.as('mom_rpmb'),
                    yoy_rpmb: sql<string>`CONCAT(ROUND(((SUM(${summaryRevCluster.revAllM})/(SUM(${rgbCluster.mtdPayload})/1024)) / (SUM(${summaryRevCluster.revAllM12})/(SUM(${rgbCluster.payloadLastYear})/1024))-1)*100,2),'%')`.as('yoy_rpmb'),
                })
                .from(clusterTerritory)
                .leftJoin(rgbCluster, eq(clusterTerritory.cluster, rgbCluster.cluster))
                .leftJoin(clusterTargetRevenue, eq(clusterTerritory.cluster, clusterTargetRevenue.cluster))
                .leftJoin(summaryRevCluster, eq(clusterTerritory.cluster, summaryRevCluster.cluster))
                .groupBy(clusterTerritory.cluster)

            const kabupatenHeaderQuery = db
                .selectDistinct({
                    name: sql<string | null>`'KABUPATEN'`,
                    target_rgb_all: sql<number>`''`,
                    mtd_rgb: sql<number>`''`,
                    mom_rgb: sql<string>`''`,
                    yoy_rgb: sql<string>`''`,
                    ach_target_fm_rgb: sql<string>`''`,
                    drr_rgb: sql<string>`''`,
                    gap_to_target_rgb: sql<string>`''`,
                    penetration_to_cb: sql<string>`''`,

                    mtd_rgb_data: sql<number>`''`,
                    mom_rgb_data: sql<string>`''`,
                    yoy_rgb_data: sql<string>`''`,

                    mtd_rgb_digital: sql<number>`''`,
                    mom_rgb_digital: sql<string>`''`,
                    yoy_rgb_digital: sql<string>`''`,

                    mtd_arpu: sql<number>`''`,
                    mom_arpu: sql<string>`''`,
                    yoy_arpu: sql<string>`''`,

                    mtd_arpu_data: sql<number>`''`,
                    mom_arpu_data: sql<string>`''`,
                    yoy_arpu_data: sql<string>`''`,

                    mtd_arpu_digital: sql<number>`''`,
                    mom_arpu_digital: sql<string>`''`,
                    yoy_arpu_digital: sql<string>`''`,

                    mtd_rpmb: sql<number>`''`,
                    mom_rpmb: sql<string>`''`,
                    yoy_rpmb: sql<string>`''`,
                })
                .from(feiTargetPuma)

            const kabupatenTerritory = db
                .select({
                    kabupaten: territoryArea4.kabupaten,
                })
                .from(territoryArea4)
                .where(branch && subbranch && cluster && kabupaten ? and(
                    eq(territoryArea4.regional, 'PUMA'),
                    eq(territoryArea4.branch, branch),
                    eq(territoryArea4.subbranch, subbranch),
                    eq(territoryArea4.cluster, cluster),
                    eq(territoryArea4.kabupaten, kabupaten),
                ) : branch && subbranch && cluster ? and(
                    eq(territoryArea4.regional, 'PUMA'),
                    eq(territoryArea4.branch, branch),
                    eq(territoryArea4.subbranch, subbranch),
                    eq(territoryArea4.cluster, cluster),
                ) : branch && subbranch ? and(
                    eq(territoryArea4.regional, 'PUMA'),
                    eq(territoryArea4.branch, branch),
                    eq(territoryArea4.subbranch, subbranch),
                ) : branch ? and(
                    eq(territoryArea4.regional, 'PUMA'),
                    eq(territoryArea4.branch, branch),
                ) : eq(territoryArea4.regional, 'PUMA'))
                .groupBy(territoryArea4.kabupaten)
                .as('a')

            const rgbKabupaten = db
                .select({
                    kabupaten: summaryRgbHqKabupaten.kabupaten,
                    cbLastYear: summaryRgbHqKabupaten.cb_m12,
                    cbLastMonth: summaryRgbHqKabupaten.cb_m1,
                    mtdCb: summaryRgbHqKabupaten.cb_mtd,
                    rgbLastYear: summaryRgbHqKabupaten.rgb_m12,
                    rgbLastMonth: summaryRgbHqKabupaten.rgb_m1,
                    mtdRgb: summaryRgbHqKabupaten.rgb_mtd,
                    rgbDataLastYear: summaryRgbHqKabupaten.rgb_data_m12,
                    rgbDataLastMonth: summaryRgbHqKabupaten.rgb_data_m1,
                    mtdRgbData: summaryRgbHqKabupaten.rgb_data_mtd,
                    rgbDigitalLastYear: summaryRgbHqKabupaten.rgb_digital_m12,
                    rgbDigitalLastMonth: summaryRgbHqKabupaten.rgb_digital_m1,
                    mtdRgbDigital: summaryRgbHqKabupaten.rgb_digital_mtd,
                    payloadLastYear: summaryRgbHqKabupaten.payload_user_m12,
                    payloadLastMonth: summaryRgbHqKabupaten.payload_user_m1,
                    mtdPayload: summaryRgbHqKabupaten.payload_user_mtd
                })
                .from(summaryRgbHqKabupaten)
                .where(and(
                    eq(summaryRgbHqKabupaten.event_date, currDate),
                    eq(summaryRgbHqKabupaten.regional, 'PUMA'),
                    eq(summaryRgbHqKabupaten.catlos, '1.0-1 mo'),
                    eq(summaryRgbHqKabupaten.brand, 'all')
                ))
                .groupBy(summaryRgbHqKabupaten.kabupaten)
                .as('b')

            const kabupatenTargetRevenue = db
                .select({
                    kabupaten: territoryArea4.kabupaten,
                    target_paying_los_0_1: sum(feiTargetPuma.paying_los_0_1).as('target_paying_los_0_1')
                })
                .from(feiTargetPuma)
                .rightJoin(territoryArea4, eq(feiTargetPuma.territory, territoryArea4.kabupaten))
                .where(eq(feiTargetPuma.periode, yyyyMM))
                .groupBy(territoryArea4.kabupaten)
                .as('c')

            const summaryRevKabupaten = db
                .select({
                    kabupaten: summaryRevAllKabupaten.kabupaten,
                    revAllM: summaryRevAllKabupaten.rev_all_m,
                    revAllM1: sql<number>`SUM(CASE WHEN ${summaryRevAllKabupaten.tgl} = ${prevDate} THEN ${summaryRevAllKabupaten.rev_all_m} END)`.as('rev_all_m1'),
                    revAllM12: sql<number>`SUM(CASE WHEN ${summaryRevAllKabupaten.tgl} = ${prevYearCurrDate} THEN ${summaryRevAllKabupaten.rev_all_m} END)`.as('rev_all_m12'),

                    revBbM: summaryRevAllKabupaten.rev_bb_m,
                    revBbM1: sql<number>`SUM(CASE WHEN ${summaryRevAllKabupaten.tgl} = ${prevDate} THEN ${summaryRevAllKabupaten.rev_bb_m} END)`.as('rev_bb_m1'),
                    revBbM12: sql<number>`SUM(CASE WHEN ${summaryRevAllKabupaten.tgl} = ${prevYearCurrDate} THEN ${summaryRevAllKabupaten.rev_bb_m} END)`.as('rev_bb_m12'),

                    revVoiceM: summaryRevAllKabupaten.rev_voice_m,
                    revVoiceM1: sql<number>`SUM(CASE WHEN ${summaryRevAllKabupaten.tgl} = ${prevDate} THEN ${summaryRevAllKabupaten.rev_voice_m} END)`.as('rev_voice_m1'),
                    revVoiceM12: sql<number>`SUM(CASE WHEN ${summaryRevAllKabupaten.tgl} = ${prevYearCurrDate} THEN ${summaryRevAllKabupaten.rev_voice_m} END)`.as('rev_voice_m12'),

                    revDigitalM: summaryRevAllKabupaten.rev_digital_m,
                    revDigitalM1: sql<number>`SUM(CASE WHEN ${summaryRevAllKabupaten.tgl} = ${prevDate} THEN ${summaryRevAllKabupaten.rev_digital_m} END)`.as('rev_digital_m1'),
                    revDigitalM12: sql<number>`SUM(CASE WHEN ${summaryRevAllKabupaten.tgl} = ${prevYearCurrDate} THEN ${summaryRevAllKabupaten.rev_digital_m} END)`.as('rev_digital_m12')
                })
                .from(summaryRevAllKabupaten)
                .where(and(
                    eq(summaryRevAllKabupaten.regional, 'PUMA'),
                    eq(summaryRevAllKabupaten.newAbcStrate, 'all')
                ))
                .groupBy(summaryRevAllKabupaten.kabupaten)
                .as('d')

            const revenueKabupaten = db
                .select({
                    name: kabupatenTerritory.kabupaten,
                    target_rgb_all: sql<number>`ROUND(SUM(${kabupatenTargetRevenue.target_paying_los_0_1}),0)`.as('target_rgb_all'),
                    mtd_rgb: sql<number>`ROUND(SUM(${rgbKabupaten.mtdRgb}),0)`.as('mtd_rgb'),
                    mom_rgb: sql<string>`CONCAT(ROUND(((SUM(${rgbKabupaten.mtdRgb})/SUM(${rgbKabupaten.rgbLastMonth}))-1)*100,2),'%')`.as('mom_rgb'),
                    yoy_rgb: sql<string>`CONCAT(ROUND(((SUM(${rgbKabupaten.mtdRgb})/SUM(${rgbKabupaten.rgbLastYear}))-1)*100,2),'%')`.as('yoy_rgb'),

                    ach_target_fm_rgb: sql<string>`CONCAT(ROUND((SUM(${rgbKabupaten.mtdRgb})/SUM(${kabupatenTargetRevenue.target_paying_los_0_1}))*100,2),'%')`.as('ach_target_fm_rgb'),
                    drr_rgb: sql<string>`CONCAT(ROUND((SUM(${rgbKabupaten.mtdRgb})/(${today}/${daysInMonth}*(SUM(${kabupatenTargetRevenue.target_paying_los_0_1}))))*100,2),'%')`.as('drr_rgb'),
                    gap_to_target_rgb: sql<string>`ROUND((COALESCE(SUM(${rgbKabupaten.mtdRgb}) - SUM(${kabupatenTargetRevenue.target_paying_los_0_1}),0)),0)`.as('gap_to_target_rgb'),
                    penetration_to_cb: sql<string>`CONCAT(ROUND((SUM(${rgbKabupaten.mtdRgb})/SUM(${rgbKabupaten.mtdCb})*100),2),'%')`.as('penetration_to_cb'),

                    mtd_rgb_data: sql<number>`ROUND(SUM(${rgbKabupaten.mtdRgbData}),0)`.as('mtd_rgb_data'),
                    mom_rgb_data: sql<string>`CONCAT(ROUND((((SUM(${rgbKabupaten.mtdRgbData})/SUM(${rgbKabupaten.rgbDataLastMonth}))-1)*100),2),'%')`.as('mom_rgb_data'),
                    yoy_rgb_data: sql<string>`CONCAT(ROUND((((SUM(${rgbKabupaten.mtdRgbData})/SUM(${rgbKabupaten.rgbDataLastYear}))-1)*100),2),'%')`.as('yoy_rgb_data'),

                    mtd_rgb_digital: sql<number>`ROUND(SUM(${rgbKabupaten.mtdRgbDigital}),0)`.as('mtd_rgb_digital'),
                    mom_rgb_digital: sql<string>`CONCAT(ROUND((((SUM(${rgbKabupaten.mtdRgbDigital})/SUM(${rgbKabupaten.rgbDigitalLastMonth}))-1)*100),2),'%')`.as('mom_rgb_digital'),
                    yoy_rgb_digital: sql<string>`CONCAT(ROUND((((SUM(${rgbKabupaten.mtdRgbDigital})/SUM(${rgbKabupaten.rgbDigitalLastYear}))-1)*100),2),'%')`.as('yoy_rgb_digital'),

                    mtd_arpu: sql<number>`ROUND(SUM(${summaryRevKabupaten.revAllM})/SUM(${rgbKabupaten.mtdRgb}),0)`.as('mtd_arpu'),
                    mom_arpu: sql<string>`CONCAT(ROUND((((SUM(${summaryRevKabupaten.revAllM})/SUM(${rgbKabupaten.mtdRgb}))/(SUM(${summaryRevKabupaten.revAllM1})/SUM(${rgbKabupaten.rgbDataLastMonth})))-1)*100,2),'%')`.as('mom_arpu'),
                    yoy_arpu: sql<string>`CONCAT(ROUND((((SUM(${summaryRevKabupaten.revAllM})/SUM(${rgbKabupaten.mtdRgb}))/(SUM(${summaryRevKabupaten.revAllM12})/SUM(${rgbKabupaten.rgbDataLastYear})))-1)*100,2),'%')`.as('yoy_arpu'),

                    mtd_arpu_data: sql<number>`ROUND(SUM(${summaryRevKabupaten.revBbM})/SUM(${rgbKabupaten.mtdRgbData}),0)`.as('mtd_arpu_data'),
                    mom_arpu_data: sql<string>`CONCAT(ROUND((((SUM(${summaryRevKabupaten.revBbM})/SUM(${rgbKabupaten.mtdRgbData}))/(SUM(${summaryRevKabupaten.revBbM1})/SUM(${rgbKabupaten.rgbDataLastMonth})))-1)*100,2),'%')`.as('mom_arpu_data'),
                    yoy_arpu_data: sql<string>`CONCAT(ROUND((((SUM(${summaryRevKabupaten.revBbM})/SUM(${rgbKabupaten.mtdRgbData}))/(SUM(${summaryRevKabupaten.revBbM12})/SUM(${rgbKabupaten.rgbDataLastYear})))-1)*100,2),'%')`.as('yoy_arpu_data'),

                    mtd_arpu_digital: sql<number>`ROUND(SUM(${summaryRevKabupaten.revDigitalM})/SUM(${rgbKabupaten.mtdRgbDigital}),0)`.as('mtd_arpu_digital'),
                    mom_arpu_digital: sql<string>`CONCAT(ROUND((((SUM(${summaryRevKabupaten.revDigitalM})/SUM(${rgbKabupaten.mtdRgbDigital}))/(SUM(${summaryRevKabupaten.revDigitalM1})/SUM(${rgbKabupaten.rgbDigitalLastMonth})))-1)*100,2),'%')`.as('mom_arpu_digital'),
                    yoy_arpu_digital: sql<string>`CONCAT(ROUND((((SUM(${summaryRevKabupaten.revDigitalM})/SUM(${rgbKabupaten.mtdRgbDigital}))/(SUM(${summaryRevKabupaten.revDigitalM12})/SUM(${rgbKabupaten.rgbDigitalLastYear})))-1)*100,2),'%')`.as('yoy_arpu_digital'),

                    mtd_rpmb: sql<number>`ROUND(SUM(${summaryRevKabupaten.revAllM})/(SUM(${rgbKabupaten.mtdPayload})/1024),2)`.as('mtd_rpmb'),
                    mom_rpmb: sql<string>`CONCAT(ROUND(((SUM(${summaryRevKabupaten.revAllM})/(SUM(${rgbKabupaten.mtdPayload})/1024)) / (SUM(${summaryRevKabupaten.revAllM1})/(SUM(${rgbKabupaten.payloadLastMonth})/1024))-1)*100,2),'%')`.as('mom_rpmb'),
                    yoy_rpmb: sql<string>`CONCAT(ROUND(((SUM(${summaryRevKabupaten.revAllM})/(SUM(${rgbKabupaten.mtdPayload})/1024)) / (SUM(${summaryRevKabupaten.revAllM12})/(SUM(${rgbKabupaten.payloadLastYear})/1024))-1)*100,2),'%')`.as('yoy_rpmb'),
                })
                .from(kabupatenTerritory)
                .leftJoin(rgbKabupaten, eq(kabupatenTerritory.kabupaten, rgbKabupaten.kabupaten))
                .leftJoin(kabupatenTargetRevenue, eq(kabupatenTerritory.kabupaten, kabupatenTargetRevenue.kabupaten))
                .leftJoin(summaryRevKabupaten, eq(kabupatenTerritory.kabupaten, summaryRevKabupaten.kabupaten))
                .groupBy(kabupatenTerritory.kabupaten)

            const [finalDataRevenue] = await Promise.all([
                unionAll(revenueRegional, branchHeaderQuery, revenueBranch, subbranchHeaderQuery, revenueSubbranch, clusterHeaderQuery, revenueCluster, kabupatenHeaderQuery, revenueKabupaten)
            ])

            return c.json(finalDataRevenue, 200)
        })
    .get('/paying-los-prabayar', zValidator('query', z.object({ date: z.coerce.date().optional() })),
        async c => {
            const { date } = c.req.valid('query')
            const selectedDate = date ? new Date(date) : subDays(new Date(), 2)

            const currDate = format(selectedDate, 'yyyy-MM-dd')
            const yyyyMM = format(selectedDate, 'yyyyMM')
            const daysInMonth = getDaysInMonth(selectedDate)
            const today = Number(format(selectedDate, 'd'))

            const regionalTerritory = db
                .select({
                    regional: territoryArea4.regional,
                    branch: territoryArea4.branch,
                    subbranch: territoryArea4.subbranch,
                    cluster: territoryArea4.cluster,
                    kabupaten: territoryArea4.kabupaten
                })
                .from(territoryArea4)
                .where(eq(territoryArea4.regional, 'PUMA'))
                .as('a')

            const rgbRegional = db
                .selectDistinct({
                    kabupaten: summaryRgbHqKabupaten.kabupaten,
                    cbLastYear: summaryRgbHqKabupaten.cb_m12,
                    cbLastMonth: summaryRgbHqKabupaten.cb_m1,
                    mtdCb: summaryRgbHqKabupaten.cb_mtd,
                    rgbLastYear: summaryRgbHqKabupaten.rgb_m12,
                    rgbLastMonth: summaryRgbHqKabupaten.rgb_m1,
                    mtdRgb: summaryRgbHqKabupaten.rgb_mtd,
                    rgbDataLastYear: summaryRgbHqKabupaten.rgb_data_m12,
                    rgbDataLastMonth: summaryRgbHqKabupaten.rgb_data_m1,
                    mtdRgbData: summaryRgbHqKabupaten.rgb_data_mtd,
                    rgbDigitalLastYear: summaryRgbHqKabupaten.rgb_digital_m12,
                    rgbDigitalLastMonth: summaryRgbHqKabupaten.rgb_digital_m1,
                    mtdRgbDigital: summaryRgbHqKabupaten.rgb_digital_mtd,
                    payloadLastYear: summaryRgbHqKabupaten.payload_user_m12,
                    payloadLastMonth: summaryRgbHqKabupaten.payload_user_m1,
                    mtdPayload: summaryRgbHqKabupaten.payload_user_mtd
                })
                .from(summaryRgbHqKabupaten)
                .where(and(
                    eq(summaryRgbHqKabupaten.event_date, currDate),
                    eq(summaryRgbHqKabupaten.regional, 'PUMA'),
                    eq(summaryRgbHqKabupaten.catlos, '1.0-1 mo'),
                    eq(summaryRgbHqKabupaten.brand, 'Telkomsel Prepaid')
                ))
                .as('b')

            const regionalTargetRevenue = db
                .select({
                    territory: feiTargetPuma.territory,
                    targetRgbAll: sum(feiTargetPuma.paying_los_0_1_prepaid).as('target_rgb_all')
                })
                .from(feiTargetPuma)
                .where(eq(feiTargetPuma.periode, yyyyMM))
                .groupBy(feiTargetPuma.territory)
                .as('c')

            const summaryRevRegional = db
                .selectDistinct({
                    kabupaten: summaryRevAllKabupaten.kabupaten,
                    revAllM: summaryRevAllKabupaten.rev_all_m,
                    revAllM1: summaryRevAllKabupaten.rev_all_m1,
                    revAllM12: summaryRevAllKabupaten.rev_all_m12,
                    revAllY: summaryRevAllKabupaten.rev_all_y,
                    revAllY1: summaryRevAllKabupaten.rev_all_y1,
                    revAllQ: summaryRevAllKabupaten.rev_all_q,
                    revAllQ1: summaryRevAllKabupaten.rev_all_q1,

                    revBbM: summaryRevAllKabupaten.rev_bb_m,
                    revBbM1: summaryRevAllKabupaten.rev_bb_m1,
                    revBbM12: summaryRevAllKabupaten.rev_bb_m12,
                    revBbY: summaryRevAllKabupaten.rev_bb_y,
                    revBbY1: summaryRevAllKabupaten.rev_bb_y1,
                    revBbQ: summaryRevAllKabupaten.rev_bb_q,
                    revBbQ1: summaryRevAllKabupaten.rev_bb_q1,

                    revVoiceM: summaryRevAllKabupaten.rev_voice_m,
                    revVoiceM1: summaryRevAllKabupaten.rev_voice_m1,
                    revVoiceM12: summaryRevAllKabupaten.rev_voice_m12,
                    revVoiceY: summaryRevAllKabupaten.rev_voice_y,
                    revVoiceY1: summaryRevAllKabupaten.rev_voice_y1,
                    revVoiceQ: summaryRevAllKabupaten.rev_voice_q,
                    revVoiceQ1: summaryRevAllKabupaten.rev_voice_q1,

                    revDigitalM: summaryRevAllKabupaten.rev_digital_m,
                    revDigitalM1: summaryRevAllKabupaten.rev_digital_m1,
                    revDigitalM12: summaryRevAllKabupaten.rev_digital_m12,
                    revDigitalY: summaryRevAllKabupaten.rev_digital_y,
                    revDigitalY1: summaryRevAllKabupaten.rev_digital_y1,
                    revDigitalQ: summaryRevAllKabupaten.rev_digital_q,
                    revDigitalQ1: summaryRevAllKabupaten.rev_digital_q1
                })
                .from(summaryRevAllKabupaten)
                .where(and(
                    eq(summaryRevAllKabupaten.tgl, currDate),
                    eq(summaryRevAllKabupaten.regional, 'PUMA'),
                ))
                .groupBy(summaryRevAllKabupaten.kabupaten)
                .as('d')

            const revenueRegional = db
                .select({
                    name: regionalTerritory.regional,
                    target_rgb_all: sql<number>`ROUND(SUM(${regionalTargetRevenue.targetRgbAll}),0)`.as('target_rgb_all'),
                    mtd_rgb: sql<number>`ROUND(SUM(${rgbRegional.mtdRgb}),0)`.as('mtd_rgb'),
                    mom_rgb: sql<string>`CONCAT(ROUND(((SUM(${rgbRegional.mtdRgb})/SUM(${rgbRegional.rgbLastMonth}))-1)*100,2),'%')`.as('mom_rgb'),
                    yoy_rgb: sql<string>`CONCAT(ROUND(((SUM(${rgbRegional.mtdRgb})/SUM(${rgbRegional.rgbLastYear}))-1)*100,2),'%')`.as('yoy_rgb'),
                    ach_target_fm_rgb: sql<string>`CONCAT(ROUND((SUM(${rgbRegional.mtdRgb})/SUM(${regionalTargetRevenue.targetRgbAll}))*100,2),'%')`.as('ach_target_fm_rgb'),
                    drr_rgb: sql<string>`CONCAT(ROUND((SUM(${rgbRegional.mtdRgb})/(${today}/${daysInMonth}*(SUM(${regionalTargetRevenue.targetRgbAll}))))*100,2),'%')`.as('drr_rgb'),
                    gap_to_target_rgb: sql<string>`ROUND((COALESCE(SUM(${rgbRegional.mtdRgb}) - SUM(${regionalTargetRevenue.targetRgbAll}),0)),0)`.as('gap_to_target_rgb'),
                    penetration_to_cb: sql<string>`CONCAT(ROUND((SUM(${rgbRegional.mtdRgb})/SUM(${rgbRegional.mtdCb})*100),2),'%')`.as('penetration_to_cb'),
                    mtd_rgb_data: sql<number>`ROUND(SUM(${rgbRegional.mtdRgbData}),0)`.as('mtd_rgb_data'),
                    mom_rgb_data: sql<string>`CONCAT(ROUND((((SUM(${rgbRegional.mtdRgbData})/SUM(${rgbRegional.rgbDataLastMonth}))-1)*100),2),'%')`.as('mom_rgb_data'),
                    yoy_rgb_data: sql<string>`CONCAT(ROUND((((SUM(${rgbRegional.mtdRgbData})/SUM(${rgbRegional.rgbDataLastYear}))-1)*100),2),'%')`.as('yoy_rgb_data'),
                    mtd_rgb_digital: sql<number>`ROUND(SUM(${rgbRegional.mtdRgbDigital}),0)`.as('mtd_rgb_digital'),
                    mom_rgb_digital: sql<string>`CONCAT(ROUND((((SUM(${rgbRegional.mtdRgbDigital})/SUM(${rgbRegional.rgbDigitalLastMonth}))-1)*100),2),'%')`.as('mom_rgb_digital'),
                    yoy_rgb_digital: sql<string>`CONCAT(ROUND((((SUM(${rgbRegional.mtdRgbDigital})/SUM(${rgbRegional.rgbDigitalLastYear}))-1)*100),2),'%')`.as('yoy_rgb_digital'),
                    mtd_arpu_data: sql<number>`ROUND(SUM(${summaryRevRegional.revBbM})/SUM(${rgbRegional.mtdRgbData}),0)`.as('mtd_arpu_data'),
                    mom_arpu_data: sql<string>`CONCAT(ROUND((((SUM(${summaryRevRegional.revBbM})/SUM(${rgbRegional.mtdRgbData}))/(SUM(${summaryRevRegional.revBbM1})/SUM(${rgbRegional.rgbDataLastMonth})))-1)*100,2),'%')`.as('mom_arpu_data'),
                    yoy_arpu_data: sql<string>`CONCAT(ROUND((((SUM(${summaryRevRegional.revBbM})/SUM(${rgbRegional.mtdRgbData}))/(SUM(${summaryRevRegional.revBbM12})/SUM(${rgbRegional.rgbDataLastYear})))-1)*100,2),'%')`.as('yoy_arpu_data'),
                    mtd_arpu_digital: sql<number>`ROUND(SUM(${summaryRevRegional.revDigitalM})/SUM(${rgbRegional.mtdRgbDigital}),0)`.as('mtd_arpu_digital'),
                    mom_arpu_digital: sql<string>`CONCAT(ROUND((((SUM(${summaryRevRegional.revDigitalM})/SUM(${rgbRegional.mtdRgbDigital}))/(SUM(${summaryRevRegional.revDigitalM1})/SUM(${rgbRegional.rgbDigitalLastMonth})))-1)*100,2),'%')`.as('mom_arpu_digital'),
                    yoy_arpu_digital: sql<string>`CONCAT(ROUND((((SUM(${summaryRevRegional.revDigitalM})/SUM(${rgbRegional.mtdRgbDigital}))/(SUM(${summaryRevRegional.revDigitalM12})/SUM(${rgbRegional.rgbDigitalLastYear})))-1)*100,2),'%')`.as('yoy_arpu_digital'),
                    mtd_rpmb: sql<number>`ROUND(SUM(${summaryRevRegional.revAllM})/(SUM(${rgbRegional.mtdPayload})/1024),2)`.as('mtd_rpmb'),
                    mom_rpmb: sql<string>`CONCAT(ROUND(((SUM(${summaryRevRegional.revAllM})/(SUM(${rgbRegional.mtdPayload})/1024)) / (SUM(${summaryRevRegional.revAllM1})/(SUM(${rgbRegional.payloadLastMonth})/1024))-1)*100,2),'%')`.as('mom_rpmb'),
                    yoy_rpmb: sql<string>`CONCAT(ROUND(((SUM(${summaryRevRegional.revAllM})/(SUM(${rgbRegional.mtdPayload})/1024)) / (SUM(${summaryRevRegional.revAllM12})/(SUM(${rgbRegional.payloadLastYear})/1024))-1)*100,2),'%')`.as('yoy_rpmb'),
                })
                .from(regionalTerritory)
                .leftJoin(rgbRegional, eq(regionalTerritory.kabupaten, rgbRegional.kabupaten))
                .leftJoin(regionalTargetRevenue, eq(regionalTerritory.kabupaten, regionalTargetRevenue.territory))
                .leftJoin(summaryRevRegional, eq(regionalTerritory.kabupaten, summaryRevRegional.kabupaten))
                .groupBy(regionalTerritory.regional)

            const branchHeaderQuery = db
                .selectDistinct({
                    name: sql<string | null>`'BRANCH'`,
                    target_rgb_all: sql<number>`''`,
                    mtd_rgb: sql<number>`''`,
                    mom_rgb: sql<string>`''`,
                    yoy_rgb: sql<string>`''`,
                    ach_target_fm_rgb: sql<string>`''`,
                    drr_rgb: sql<string>`''`,
                    gap_to_target_rgb: sql<string>`''`,
                    penetration_to_cb: sql<string>`''`,

                    mtd_rgb_data: sql<number>`''`,
                    mom_rgb_data: sql<string>`''`,
                    yoy_rgb_data: sql<string>`''`,

                    mtd_rgb_digital: sql<number>`''`,
                    mom_rgb_digital: sql<string>`''`,
                    yoy_rgb_digital: sql<string>`''`,

                    mtd_arpu_data: sql<number>`''`,
                    mom_arpu_data: sql<string>`''`,
                    yoy_arpu_data: sql<string>`''`,

                    mtd_arpu_digital: sql<number>`''`,
                    mom_arpu_digital: sql<string>`''`,
                    yoy_arpu_digital: sql<string>`''`,

                    mtd_rpmb: sql<number>`''`,
                    mom_rpmb: sql<string>`''`,
                    yoy_rpmb: sql<string>`''`,
                })
                .from(feiTargetPuma)

            const revenueBranch = db
                .select({
                    name: regionalTerritory.branch,
                    target_rgb_all: sql<number>`ROUND(SUM(${regionalTargetRevenue.targetRgbAll}),0)`.as('target_rgb_all'),
                    mtd_rgb: sql<number>`ROUND(SUM(${rgbRegional.mtdRgb}),0)`.as('mtd_rgb'),
                    mom_rgb: sql<string>`CONCAT(ROUND(((SUM(${rgbRegional.mtdRgb})/SUM(${rgbRegional.rgbLastMonth}))-1)*100,2),'%')`.as('mom_rgb'),
                    yoy_rgb: sql<string>`CONCAT(ROUND(((SUM(${rgbRegional.mtdRgb})/SUM(${rgbRegional.rgbLastYear}))-1)*100,2),'%')`.as('yoy_rgb'),
                    ach_target_fm_rgb: sql<string>`CONCAT(ROUND((SUM(${rgbRegional.mtdRgb})/SUM(${regionalTargetRevenue.targetRgbAll}))*100,2),'%')`.as('ach_target_fm_rgb'),
                    drr_rgb: sql<string>`CONCAT(ROUND((SUM(${rgbRegional.mtdRgb})/(${today}/${daysInMonth}*(SUM(${regionalTargetRevenue.targetRgbAll}))))*100,2),'%')`.as('drr_rgb'),
                    gap_to_target_rgb: sql<string>`ROUND((COALESCE(SUM(${rgbRegional.mtdRgb}) - SUM(${regionalTargetRevenue.targetRgbAll}),0)),0)`.as('gap_to_target_rgb'),
                    penetration_to_cb: sql<string>`CONCAT(ROUND((SUM(${rgbRegional.mtdRgb})/SUM(${rgbRegional.mtdCb})*100),2),'%')`.as('penetration_to_cb'),

                    mtd_rgb_data: sql<number>`ROUND(SUM(${rgbRegional.mtdRgbData}),0)`.as('mtd_rgb_data'),
                    mom_rgb_data: sql<string>`CONCAT(ROUND((((SUM(${rgbRegional.mtdRgbData})/SUM(${rgbRegional.rgbDataLastMonth}))-1)*100),2),'%')`.as('mom_rgb_data'),
                    yoy_rgb_data: sql<string>`CONCAT(ROUND((((SUM(${rgbRegional.mtdRgbData})/SUM(${rgbRegional.rgbDataLastYear}))-1)*100),2),'%')`.as('yoy_rgb_data'),

                    mtd_rgb_digital: sql<number>`ROUND(SUM(${rgbRegional.mtdRgbDigital}),0)`.as('mtd_rgb_digital'),
                    mom_rgb_digital: sql<string>`CONCAT(ROUND((((SUM(${rgbRegional.mtdRgbDigital})/SUM(${rgbRegional.rgbDigitalLastMonth}))-1)*100),2),'%')`.as('mom_rgb_digital'),
                    yoy_rgb_digital: sql<string>`CONCAT(ROUND((((SUM(${rgbRegional.mtdRgbDigital})/SUM(${rgbRegional.rgbDigitalLastYear}))-1)*100),2),'%')`.as('yoy_rgb_digital'),

                    mtd_arpu_data: sql<number>`ROUND(SUM(${summaryRevRegional.revBbM})/SUM(${rgbRegional.mtdRgbData}),0)`.as('mtd_arpu_data'),
                    mom_arpu_data: sql<string>`CONCAT(ROUND((((SUM(${summaryRevRegional.revBbM})/SUM(${rgbRegional.mtdRgbData}))/(SUM(${summaryRevRegional.revBbM1})/SUM(${rgbRegional.rgbDataLastMonth})))-1)*100,2),'%')`.as('mom_arpu_data'),
                    yoy_arpu_data: sql<string>`CONCAT(ROUND((((SUM(${summaryRevRegional.revBbM})/SUM(${rgbRegional.mtdRgbData}))/(SUM(${summaryRevRegional.revBbM12})/SUM(${rgbRegional.rgbDataLastYear})))-1)*100,2),'%')`.as('yoy_arpu_data'),

                    mtd_arpu_digital: sql<number>`ROUND(SUM(${summaryRevRegional.revDigitalM})/SUM(${rgbRegional.mtdRgbDigital}),0)`.as('mtd_arpu_digital'),
                    mom_arpu_digital: sql<string>`CONCAT(ROUND((((SUM(${summaryRevRegional.revDigitalM})/SUM(${rgbRegional.mtdRgbDigital}))/(SUM(${summaryRevRegional.revDigitalM1})/SUM(${rgbRegional.rgbDigitalLastMonth})))-1)*100,2),'%')`.as('mom_arpu_digital'),
                    yoy_arpu_digital: sql<string>`CONCAT(ROUND((((SUM(${summaryRevRegional.revDigitalM})/SUM(${rgbRegional.mtdRgbDigital}))/(SUM(${summaryRevRegional.revDigitalM12})/SUM(${rgbRegional.rgbDigitalLastYear})))-1)*100,2),'%')`.as('yoy_arpu_digital'),

                    mtd_rpmb: sql<number>`ROUND(SUM(${summaryRevRegional.revAllM})/(SUM(${rgbRegional.mtdPayload})/1024),2)`.as('mtd_rpmb'),
                    mom_rpmb: sql<string>`CONCAT(ROUND(((SUM(${summaryRevRegional.revAllM})/(SUM(${rgbRegional.mtdPayload})/1024)) / (SUM(${summaryRevRegional.revAllM1})/(SUM(${rgbRegional.payloadLastMonth})/1024))-1)*100,2),'%')`.as('mom_rpmb'),
                    yoy_rpmb: sql<string>`CONCAT(ROUND(((SUM(${summaryRevRegional.revAllM})/(SUM(${rgbRegional.mtdPayload})/1024)) / (SUM(${summaryRevRegional.revAllM12})/(SUM(${rgbRegional.payloadLastYear})/1024))-1)*100,2),'%')`.as('yoy_rpmb'),
                })
                .from(regionalTerritory)
                .leftJoin(rgbRegional, eq(regionalTerritory.kabupaten, rgbRegional.kabupaten))
                .leftJoin(regionalTargetRevenue, eq(regionalTerritory.kabupaten, regionalTargetRevenue.territory))
                .leftJoin(summaryRevRegional, eq(regionalTerritory.kabupaten, summaryRevRegional.kabupaten))
                .groupBy(regionalTerritory.branch)

            const subbranchHeaderQuery = db
                .selectDistinct({
                    name: sql<string | null>`'SUBBRANCH'`,
                    target_rgb_all: sql<number>`''`,
                    mtd_rgb: sql<number>`''`,
                    mom_rgb: sql<string>`''`,
                    yoy_rgb: sql<string>`''`,
                    ach_target_fm_rgb: sql<string>`''`,
                    drr_rgb: sql<string>`''`,
                    gap_to_target_rgb: sql<string>`''`,
                    penetration_to_cb: sql<string>`''`,

                    mtd_rgb_data: sql<number>`''`,
                    mom_rgb_data: sql<string>`''`,
                    yoy_rgb_data: sql<string>`''`,

                    mtd_rgb_digital: sql<number>`''`,
                    mom_rgb_digital: sql<string>`''`,
                    yoy_rgb_digital: sql<string>`''`,

                    mtd_arpu_data: sql<number>`''`,
                    mom_arpu_data: sql<string>`''`,
                    yoy_arpu_data: sql<string>`''`,

                    mtd_arpu_digital: sql<number>`''`,
                    mom_arpu_digital: sql<string>`''`,
                    yoy_arpu_digital: sql<string>`''`,

                    mtd_rpmb: sql<number>`''`,
                    mom_rpmb: sql<string>`''`,
                    yoy_rpmb: sql<string>`''`,
                })
                .from(feiTargetPuma)

            const revenueSubbranch = db
                .select({
                    name: regionalTerritory.subbranch,
                    target_rgb_all: sql<number>`ROUND(SUM(${regionalTargetRevenue.targetRgbAll}),0)`.as('target_rgb_all'),
                    mtd_rgb: sql<number>`ROUND(SUM(${rgbRegional.mtdRgb}),0)`.as('mtd_rgb'),
                    mom_rgb: sql<string>`CONCAT(ROUND(((SUM(${rgbRegional.mtdRgb})/SUM(${rgbRegional.rgbLastMonth}))-1)*100,2),'%')`.as('mom_rgb'),
                    yoy_rgb: sql<string>`CONCAT(ROUND(((SUM(${rgbRegional.mtdRgb})/SUM(${rgbRegional.rgbLastYear}))-1)*100,2),'%')`.as('yoy_rgb'),
                    ach_target_fm_rgb: sql<string>`CONCAT(ROUND((SUM(${rgbRegional.mtdRgb})/SUM(${regionalTargetRevenue.targetRgbAll}))*100,2),'%')`.as('ach_target_fm_rgb'),
                    drr_rgb: sql<string>`CONCAT(ROUND((SUM(${rgbRegional.mtdRgb})/(${today}/${daysInMonth}*(SUM(${regionalTargetRevenue.targetRgbAll}))))*100,2),'%')`.as('drr_rgb'),
                    gap_to_target_rgb: sql<string>`ROUND((COALESCE(SUM(${rgbRegional.mtdRgb}) - SUM(${regionalTargetRevenue.targetRgbAll}),0)),0)`.as('gap_to_target_rgb'),
                    penetration_to_cb: sql<string>`CONCAT(ROUND((SUM(${rgbRegional.mtdRgb})/SUM(${rgbRegional.mtdCb})*100),2),'%')`.as('penetration_to_cb'),

                    mtd_rgb_data: sql<number>`ROUND(SUM(${rgbRegional.mtdRgbData}),0)`.as('mtd_rgb_data'),
                    mom_rgb_data: sql<string>`CONCAT(ROUND((((SUM(${rgbRegional.mtdRgbData})/SUM(${rgbRegional.rgbDataLastMonth}))-1)*100),2),'%')`.as('mom_rgb_data'),
                    yoy_rgb_data: sql<string>`CONCAT(ROUND((((SUM(${rgbRegional.mtdRgbData})/SUM(${rgbRegional.rgbDataLastYear}))-1)*100),2),'%')`.as('yoy_rgb_data'),

                    mtd_rgb_digital: sql<number>`ROUND(SUM(${rgbRegional.mtdRgbDigital}),0)`.as('mtd_rgb_digital'),
                    mom_rgb_digital: sql<string>`CONCAT(ROUND((((SUM(${rgbRegional.mtdRgbDigital})/SUM(${rgbRegional.rgbDigitalLastMonth}))-1)*100),2),'%')`.as('mom_rgb_digital'),
                    yoy_rgb_digital: sql<string>`CONCAT(ROUND((((SUM(${rgbRegional.mtdRgbDigital})/SUM(${rgbRegional.rgbDigitalLastYear}))-1)*100),2),'%')`.as('yoy_rgb_digital'),

                    mtd_arpu_data: sql<number>`ROUND(SUM(${summaryRevRegional.revBbM})/SUM(${rgbRegional.mtdRgbData}),0)`.as('mtd_arpu_data'),
                    mom_arpu_data: sql<string>`CONCAT(ROUND((((SUM(${summaryRevRegional.revBbM})/SUM(${rgbRegional.mtdRgbData}))/(SUM(${summaryRevRegional.revBbM1})/SUM(${rgbRegional.rgbDataLastMonth})))-1)*100,2),'%')`.as('mom_arpu_data'),
                    yoy_arpu_data: sql<string>`CONCAT(ROUND((((SUM(${summaryRevRegional.revBbM})/SUM(${rgbRegional.mtdRgbData}))/(SUM(${summaryRevRegional.revBbM12})/SUM(${rgbRegional.rgbDataLastYear})))-1)*100,2),'%')`.as('yoy_arpu_data'),

                    mtd_arpu_digital: sql<number>`ROUND(SUM(${summaryRevRegional.revDigitalM})/SUM(${rgbRegional.mtdRgbDigital}),0)`.as('mtd_arpu_digital'),
                    mom_arpu_digital: sql<string>`CONCAT(ROUND((((SUM(${summaryRevRegional.revDigitalM})/SUM(${rgbRegional.mtdRgbDigital}))/(SUM(${summaryRevRegional.revDigitalM1})/SUM(${rgbRegional.rgbDigitalLastMonth})))-1)*100,2),'%')`.as('mom_arpu_digital'),
                    yoy_arpu_digital: sql<string>`CONCAT(ROUND((((SUM(${summaryRevRegional.revDigitalM})/SUM(${rgbRegional.mtdRgbDigital}))/(SUM(${summaryRevRegional.revDigitalM12})/SUM(${rgbRegional.rgbDigitalLastYear})))-1)*100,2),'%')`.as('yoy_arpu_digital'),

                    mtd_rpmb: sql<number>`ROUND(SUM(${summaryRevRegional.revAllM})/(SUM(${rgbRegional.mtdPayload})/1024),2)`.as('mtd_rpmb'),
                    mom_rpmb: sql<string>`CONCAT(ROUND(((SUM(${summaryRevRegional.revAllM})/(SUM(${rgbRegional.mtdPayload})/1024)) / (SUM(${summaryRevRegional.revAllM1})/(SUM(${rgbRegional.payloadLastMonth})/1024))-1)*100,2),'%')`.as('mom_rpmb'),
                    yoy_rpmb: sql<string>`CONCAT(ROUND(((SUM(${summaryRevRegional.revAllM})/(SUM(${rgbRegional.mtdPayload})/1024)) / (SUM(${summaryRevRegional.revAllM12})/(SUM(${rgbRegional.payloadLastYear})/1024))-1)*100,2),'%')`.as('yoy_rpmb'),
                })
                .from(regionalTerritory)
                .leftJoin(rgbRegional, eq(regionalTerritory.kabupaten, rgbRegional.kabupaten))
                .leftJoin(regionalTargetRevenue, eq(regionalTerritory.kabupaten, regionalTargetRevenue.territory))
                .leftJoin(summaryRevRegional, eq(regionalTerritory.kabupaten, summaryRevRegional.kabupaten))
                .groupBy(regionalTerritory.subbranch)

            const clusterHeaderQuery = db
                .selectDistinct({
                    name: sql<string | null>`'CLUSTER'`,
                    target_rgb_all: sql<number>`''`,
                    mtd_rgb: sql<number>`''`,
                    mom_rgb: sql<string>`''`,
                    yoy_rgb: sql<string>`''`,
                    ach_target_fm_rgb: sql<string>`''`,
                    drr_rgb: sql<string>`''`,
                    gap_to_target_rgb: sql<string>`''`,
                    penetration_to_cb: sql<string>`''`,

                    mtd_rgb_data: sql<number>`''`,
                    mom_rgb_data: sql<string>`''`,
                    yoy_rgb_data: sql<string>`''`,

                    mtd_rgb_digital: sql<number>`''`,
                    mom_rgb_digital: sql<string>`''`,
                    yoy_rgb_digital: sql<string>`''`,

                    mtd_arpu_data: sql<number>`''`,
                    mom_arpu_data: sql<string>`''`,
                    yoy_arpu_data: sql<string>`''`,

                    mtd_arpu_digital: sql<number>`''`,
                    mom_arpu_digital: sql<string>`''`,
                    yoy_arpu_digital: sql<string>`''`,

                    mtd_rpmb: sql<number>`''`,
                    mom_rpmb: sql<string>`''`,
                    yoy_rpmb: sql<string>`''`,
                })
                .from(feiTargetPuma)

            const revenueCluster = db
                .select({
                    name: regionalTerritory.cluster,
                    target_rgb_all: sql<number>`ROUND(SUM(${regionalTargetRevenue.targetRgbAll}),0)`.as('target_rgb_all'),
                    mtd_rgb: sql<number>`ROUND(SUM(${rgbRegional.mtdRgb}),0)`.as('mtd_rgb'),
                    mom_rgb: sql<string>`CONCAT(ROUND(((SUM(${rgbRegional.mtdRgb})/SUM(${rgbRegional.rgbLastMonth}))-1)*100,2),'%')`.as('mom_rgb'),
                    yoy_rgb: sql<string>`CONCAT(ROUND(((SUM(${rgbRegional.mtdRgb})/SUM(${rgbRegional.rgbLastYear}))-1)*100,2),'%')`.as('yoy_rgb'),
                    ach_target_fm_rgb: sql<string>`CONCAT(ROUND((SUM(${rgbRegional.mtdRgb})/SUM(${regionalTargetRevenue.targetRgbAll}))*100,2),'%')`.as('ach_target_fm_rgb'),
                    drr_rgb: sql<string>`CONCAT(ROUND((SUM(${rgbRegional.mtdRgb})/(${today}/${daysInMonth}*(SUM(${regionalTargetRevenue.targetRgbAll}))))*100,2),'%')`.as('drr_rgb'),
                    gap_to_target_rgb: sql<string>`ROUND((COALESCE(SUM(${rgbRegional.mtdRgb}) - SUM(${regionalTargetRevenue.targetRgbAll}),0)),0)`.as('gap_to_target_rgb'),
                    penetration_to_cb: sql<string>`CONCAT(ROUND((SUM(${rgbRegional.mtdRgb})/SUM(${rgbRegional.mtdCb})*100),2),'%')`.as('penetration_to_cb'),

                    mtd_rgb_data: sql<number>`ROUND(SUM(${rgbRegional.mtdRgbData}),0)`.as('mtd_rgb_data'),
                    mom_rgb_data: sql<string>`CONCAT(ROUND((((SUM(${rgbRegional.mtdRgbData})/SUM(${rgbRegional.rgbDataLastMonth}))-1)*100),2),'%')`.as('mom_rgb_data'),
                    yoy_rgb_data: sql<string>`CONCAT(ROUND((((SUM(${rgbRegional.mtdRgbData})/SUM(${rgbRegional.rgbDataLastYear}))-1)*100),2),'%')`.as('yoy_rgb_data'),

                    mtd_rgb_digital: sql<number>`ROUND(SUM(${rgbRegional.mtdRgbDigital}),0)`.as('mtd_rgb_digital'),
                    mom_rgb_digital: sql<string>`CONCAT(ROUND((((SUM(${rgbRegional.mtdRgbDigital})/SUM(${rgbRegional.rgbDigitalLastMonth}))-1)*100),2),'%')`.as('mom_rgb_digital'),
                    yoy_rgb_digital: sql<string>`CONCAT(ROUND((((SUM(${rgbRegional.mtdRgbDigital})/SUM(${rgbRegional.rgbDigitalLastYear}))-1)*100),2),'%')`.as('yoy_rgb_digital'),

                    mtd_arpu_data: sql<number>`ROUND(SUM(${summaryRevRegional.revBbM})/SUM(${rgbRegional.mtdRgbData}),0)`.as('mtd_arpu_data'),
                    mom_arpu_data: sql<string>`CONCAT(ROUND((((SUM(${summaryRevRegional.revBbM})/SUM(${rgbRegional.mtdRgbData}))/(SUM(${summaryRevRegional.revBbM1})/SUM(${rgbRegional.rgbDataLastMonth})))-1)*100,2),'%')`.as('mom_arpu_data'),
                    yoy_arpu_data: sql<string>`CONCAT(ROUND((((SUM(${summaryRevRegional.revBbM})/SUM(${rgbRegional.mtdRgbData}))/(SUM(${summaryRevRegional.revBbM12})/SUM(${rgbRegional.rgbDataLastYear})))-1)*100,2),'%')`.as('yoy_arpu_data'),

                    mtd_arpu_digital: sql<number>`ROUND(SUM(${summaryRevRegional.revDigitalM})/SUM(${rgbRegional.mtdRgbDigital}),0)`.as('mtd_arpu_digital'),
                    mom_arpu_digital: sql<string>`CONCAT(ROUND((((SUM(${summaryRevRegional.revDigitalM})/SUM(${rgbRegional.mtdRgbDigital}))/(SUM(${summaryRevRegional.revDigitalM1})/SUM(${rgbRegional.rgbDigitalLastMonth})))-1)*100,2),'%')`.as('mom_arpu_digital'),
                    yoy_arpu_digital: sql<string>`CONCAT(ROUND((((SUM(${summaryRevRegional.revDigitalM})/SUM(${rgbRegional.mtdRgbDigital}))/(SUM(${summaryRevRegional.revDigitalM12})/SUM(${rgbRegional.rgbDigitalLastYear})))-1)*100,2),'%')`.as('yoy_arpu_digital'),

                    mtd_rpmb: sql<number>`ROUND(SUM(${summaryRevRegional.revAllM})/(SUM(${rgbRegional.mtdPayload})/1024),2)`.as('mtd_rpmb'),
                    mom_rpmb: sql<string>`CONCAT(ROUND(((SUM(${summaryRevRegional.revAllM})/(SUM(${rgbRegional.mtdPayload})/1024)) / (SUM(${summaryRevRegional.revAllM1})/(SUM(${rgbRegional.payloadLastMonth})/1024))-1)*100,2),'%')`.as('mom_rpmb'),
                    yoy_rpmb: sql<string>`CONCAT(ROUND(((SUM(${summaryRevRegional.revAllM})/(SUM(${rgbRegional.mtdPayload})/1024)) / (SUM(${summaryRevRegional.revAllM12})/(SUM(${rgbRegional.payloadLastYear})/1024))-1)*100,2),'%')`.as('yoy_rpmb'),
                })
                .from(regionalTerritory)
                .leftJoin(rgbRegional, eq(regionalTerritory.kabupaten, rgbRegional.kabupaten))
                .leftJoin(regionalTargetRevenue, eq(regionalTerritory.kabupaten, regionalTargetRevenue.territory))
                .leftJoin(summaryRevRegional, eq(regionalTerritory.kabupaten, summaryRevRegional.kabupaten))
                .groupBy(regionalTerritory.cluster)

            const kabupatenHeaderQuery = db
                .selectDistinct({
                    name: sql<string | null>`'KABUPATEN'`,
                    target_rgb_all: sql<number>`''`,
                    mtd_rgb: sql<number>`''`,
                    mom_rgb: sql<string>`''`,
                    yoy_rgb: sql<string>`''`,
                    ach_target_fm_rgb: sql<string>`''`,
                    drr_rgb: sql<string>`''`,
                    gap_to_target_rgb: sql<string>`''`,
                    penetration_to_cb: sql<string>`''`,

                    mtd_rgb_data: sql<number>`''`,
                    mom_rgb_data: sql<string>`''`,
                    yoy_rgb_data: sql<string>`''`,

                    mtd_rgb_digital: sql<number>`''`,
                    mom_rgb_digital: sql<string>`''`,
                    yoy_rgb_digital: sql<string>`''`,

                    mtd_arpu_data: sql<number>`''`,
                    mom_arpu_data: sql<string>`''`,
                    yoy_arpu_data: sql<string>`''`,

                    mtd_arpu_digital: sql<number>`''`,
                    mom_arpu_digital: sql<string>`''`,
                    yoy_arpu_digital: sql<string>`''`,

                    mtd_rpmb: sql<number>`''`,
                    mom_rpmb: sql<string>`''`,
                    yoy_rpmb: sql<string>`''`,
                })
                .from(feiTargetPuma)

            const revenueKabupaten = db
                .select({
                    name: regionalTerritory.kabupaten,
                    target_rgb_all: sql<number>`ROUND(SUM(${regionalTargetRevenue.targetRgbAll}),0)`.as('target_rgb_all'),
                    mtd_rgb: sql<number>`ROUND(SUM(${rgbRegional.mtdRgb}),0)`.as('mtd_rgb'),
                    mom_rgb: sql<string>`CONCAT(ROUND(((SUM(${rgbRegional.mtdRgb})/SUM(${rgbRegional.rgbLastMonth}))-1)*100,2),'%')`.as('mom_rgb'),
                    yoy_rgb: sql<string>`CONCAT(ROUND(((SUM(${rgbRegional.mtdRgb})/SUM(${rgbRegional.rgbLastYear}))-1)*100,2),'%')`.as('yoy_rgb'),
                    ach_target_fm_rgb: sql<string>`CONCAT(ROUND((SUM(${rgbRegional.mtdRgb})/SUM(${regionalTargetRevenue.targetRgbAll}))*100,2),'%')`.as('ach_target_fm_rgb'),
                    drr_rgb: sql<string>`CONCAT(ROUND((SUM(${rgbRegional.mtdRgb})/(${today}/${daysInMonth}*(SUM(${regionalTargetRevenue.targetRgbAll}))))*100,2),'%')`.as('drr_rgb'),
                    gap_to_target_rgb: sql<string>`ROUND((COALESCE(SUM(${rgbRegional.mtdRgb}) - SUM(${regionalTargetRevenue.targetRgbAll}),0)),0)`.as('gap_to_target_rgb'),
                    penetration_to_cb: sql<string>`CONCAT(ROUND((SUM(${rgbRegional.mtdRgb})/SUM(${rgbRegional.mtdCb})*100),2),'%')`.as('penetration_to_cb'),

                    mtd_rgb_data: sql<number>`ROUND(SUM(${rgbRegional.mtdRgbData}),0)`.as('mtd_rgb_data'),
                    mom_rgb_data: sql<string>`CONCAT(ROUND((((SUM(${rgbRegional.mtdRgbData})/SUM(${rgbRegional.rgbDataLastMonth}))-1)*100),2),'%')`.as('mom_rgb_data'),
                    yoy_rgb_data: sql<string>`CONCAT(ROUND((((SUM(${rgbRegional.mtdRgbData})/SUM(${rgbRegional.rgbDataLastYear}))-1)*100),2),'%')`.as('yoy_rgb_data'),

                    mtd_rgb_digital: sql<number>`ROUND(SUM(${rgbRegional.mtdRgbDigital}),0)`.as('mtd_rgb_digital'),
                    mom_rgb_digital: sql<string>`CONCAT(ROUND((((SUM(${rgbRegional.mtdRgbDigital})/SUM(${rgbRegional.rgbDigitalLastMonth}))-1)*100),2),'%')`.as('mom_rgb_digital'),
                    yoy_rgb_digital: sql<string>`CONCAT(ROUND((((SUM(${rgbRegional.mtdRgbDigital})/SUM(${rgbRegional.rgbDigitalLastYear}))-1)*100),2),'%')`.as('yoy_rgb_digital'),

                    mtd_arpu_data: sql<number>`ROUND(SUM(${summaryRevRegional.revBbM})/SUM(${rgbRegional.mtdRgbData}),0)`.as('mtd_arpu_data'),
                    mom_arpu_data: sql<string>`CONCAT(ROUND((((SUM(${summaryRevRegional.revBbM})/SUM(${rgbRegional.mtdRgbData}))/(SUM(${summaryRevRegional.revBbM1})/SUM(${rgbRegional.rgbDataLastMonth})))-1)*100,2),'%')`.as('mom_arpu_data'),
                    yoy_arpu_data: sql<string>`CONCAT(ROUND((((SUM(${summaryRevRegional.revBbM})/SUM(${rgbRegional.mtdRgbData}))/(SUM(${summaryRevRegional.revBbM12})/SUM(${rgbRegional.rgbDataLastYear})))-1)*100,2),'%')`.as('yoy_arpu_data'),

                    mtd_arpu_digital: sql<number>`ROUND(SUM(${summaryRevRegional.revDigitalM})/SUM(${rgbRegional.mtdRgbDigital}),0)`.as('mtd_arpu_digital'),
                    mom_arpu_digital: sql<string>`CONCAT(ROUND((((SUM(${summaryRevRegional.revDigitalM})/SUM(${rgbRegional.mtdRgbDigital}))/(SUM(${summaryRevRegional.revDigitalM1})/SUM(${rgbRegional.rgbDigitalLastMonth})))-1)*100,2),'%')`.as('mom_arpu_digital'),
                    yoy_arpu_digital: sql<string>`CONCAT(ROUND((((SUM(${summaryRevRegional.revDigitalM})/SUM(${rgbRegional.mtdRgbDigital}))/(SUM(${summaryRevRegional.revDigitalM12})/SUM(${rgbRegional.rgbDigitalLastYear})))-1)*100,2),'%')`.as('yoy_arpu_digital'),

                    mtd_rpmb: sql<number>`ROUND(SUM(${summaryRevRegional.revAllM})/(SUM(${rgbRegional.mtdPayload})/1024),2)`.as('mtd_rpmb'),
                    mom_rpmb: sql<string>`CONCAT(ROUND(((SUM(${summaryRevRegional.revAllM})/(SUM(${rgbRegional.mtdPayload})/1024)) / (SUM(${summaryRevRegional.revAllM1})/(SUM(${rgbRegional.payloadLastMonth})/1024))-1)*100,2),'%')`.as('mom_rpmb'),
                    yoy_rpmb: sql<string>`CONCAT(ROUND(((SUM(${summaryRevRegional.revAllM})/(SUM(${rgbRegional.mtdPayload})/1024)) / (SUM(${summaryRevRegional.revAllM12})/(SUM(${rgbRegional.payloadLastYear})/1024))-1)*100,2),'%')`.as('yoy_rpmb'),
                })
                .from(regionalTerritory)
                .leftJoin(rgbRegional, eq(regionalTerritory.kabupaten, rgbRegional.kabupaten))
                .leftJoin(regionalTargetRevenue, eq(regionalTerritory.kabupaten, regionalTargetRevenue.territory))
                .leftJoin(summaryRevRegional, eq(regionalTerritory.kabupaten, summaryRevRegional.kabupaten))
                .groupBy(regionalTerritory.kabupaten)

            const [finalDataRevenue] = await Promise.all([
                unionAll(revenueRegional, branchHeaderQuery, revenueBranch, subbranchHeaderQuery, revenueSubbranch, clusterHeaderQuery, revenueCluster, kabupatenHeaderQuery, revenueKabupaten)
            ])

            return c.json({ data: finalDataRevenue }, 200)
        })
    .get('/paying-los-byu', zValidator('query', z.object({ date: z.coerce.date().optional() })),
        async c => {
            const { date } = c.req.valid('query')
            const selectedDate = date ? new Date(date) : subDays(new Date(), 2)

            const currDate = format(selectedDate, 'yyyy-MM-dd')
            const yyyyMM = format(selectedDate, 'yyyyMM')
            const daysInMonth = getDaysInMonth(selectedDate)
            const today = Number(format(selectedDate, 'd'))

            const regionalTerritory = db
                .select({
                    regional: territoryArea4.regional,
                    branch: territoryArea4.branch,
                    subbranch: territoryArea4.subbranch,
                    cluster: territoryArea4.cluster,
                    kabupaten: territoryArea4.kabupaten
                })
                .from(territoryArea4)
                .where(eq(territoryArea4.regional, 'PUMA'))
                .as('a')

            const rgbRegional = db
                .selectDistinct({
                    kabupaten: summaryRgbHqKabupaten.kabupaten,
                    cbLastYear: summaryRgbHqKabupaten.cb_m12,
                    cbLastMonth: summaryRgbHqKabupaten.cb_m1,
                    mtdCb: summaryRgbHqKabupaten.cb_mtd,
                    rgbLastYear: summaryRgbHqKabupaten.rgb_m12,
                    rgbLastMonth: summaryRgbHqKabupaten.rgb_m1,
                    mtdRgb: summaryRgbHqKabupaten.rgb_mtd,
                    rgbDataLastYear: summaryRgbHqKabupaten.rgb_data_m12,
                    rgbDataLastMonth: summaryRgbHqKabupaten.rgb_data_m1,
                    mtdRgbData: summaryRgbHqKabupaten.rgb_data_mtd,
                    rgbDigitalLastYear: summaryRgbHqKabupaten.rgb_digital_m12,
                    rgbDigitalLastMonth: summaryRgbHqKabupaten.rgb_digital_m1,
                    mtdRgbDigital: summaryRgbHqKabupaten.rgb_digital_mtd,
                    payloadLastYear: summaryRgbHqKabupaten.payload_user_m12,
                    payloadLastMonth: summaryRgbHqKabupaten.payload_user_m1,
                    mtdPayload: summaryRgbHqKabupaten.payload_user_mtd
                })
                .from(summaryRgbHqKabupaten)
                .where(and(
                    eq(summaryRgbHqKabupaten.event_date, currDate),
                    eq(summaryRgbHqKabupaten.regional, 'PUMA'),
                    eq(summaryRgbHqKabupaten.catlos, '1.0-1 mo'),
                    eq(summaryRgbHqKabupaten.brand, 'byu')
                ))
                .as('b')

            const regionalTargetRevenue = db
                .select({
                    territory: feiTargetPuma.territory,
                    targetRgbAll: sum(feiTargetPuma.paying_los_0_1_byu).as('target_rgb_all')
                })
                .from(feiTargetPuma)
                .where(eq(feiTargetPuma.periode, yyyyMM))
                .groupBy(feiTargetPuma.territory)
                .as('c')

            const summaryRevRegional = db
                .selectDistinct({
                    kabupaten: summaryRevAllKabupaten.kabupaten,
                    revAllM: summaryRevAllKabupaten.rev_all_m,
                    revAllM1: summaryRevAllKabupaten.rev_all_m1,
                    revAllM12: summaryRevAllKabupaten.rev_all_m12,
                    revAllY: summaryRevAllKabupaten.rev_all_y,
                    revAllY1: summaryRevAllKabupaten.rev_all_y1,
                    revAllQ: summaryRevAllKabupaten.rev_all_q,
                    revAllQ1: summaryRevAllKabupaten.rev_all_q1,

                    revBbM: summaryRevAllKabupaten.rev_bb_m,
                    revBbM1: summaryRevAllKabupaten.rev_bb_m1,
                    revBbM12: summaryRevAllKabupaten.rev_bb_m12,
                    revBbY: summaryRevAllKabupaten.rev_bb_y,
                    revBbY1: summaryRevAllKabupaten.rev_bb_y1,
                    revBbQ: summaryRevAllKabupaten.rev_bb_q,
                    revBbQ1: summaryRevAllKabupaten.rev_bb_q1,

                    revVoiceM: summaryRevAllKabupaten.rev_voice_m,
                    revVoiceM1: summaryRevAllKabupaten.rev_voice_m1,
                    revVoiceM12: summaryRevAllKabupaten.rev_voice_m12,
                    revVoiceY: summaryRevAllKabupaten.rev_voice_y,
                    revVoiceY1: summaryRevAllKabupaten.rev_voice_y1,
                    revVoiceQ: summaryRevAllKabupaten.rev_voice_q,
                    revVoiceQ1: summaryRevAllKabupaten.rev_voice_q1,

                    revDigitalM: summaryRevAllKabupaten.rev_digital_m,
                    revDigitalM1: summaryRevAllKabupaten.rev_digital_m1,
                    revDigitalM12: summaryRevAllKabupaten.rev_digital_m12,
                    revDigitalY: summaryRevAllKabupaten.rev_digital_y,
                    revDigitalY1: summaryRevAllKabupaten.rev_digital_y1,
                    revDigitalQ: summaryRevAllKabupaten.rev_digital_q,
                    revDigitalQ1: summaryRevAllKabupaten.rev_digital_q1
                })
                .from(summaryRevAllKabupaten)
                .where(and(
                    eq(summaryRevAllKabupaten.tgl, currDate),
                    eq(summaryRevAllKabupaten.regional, 'PUMA'),
                ))
                .groupBy(summaryRevAllKabupaten.kabupaten)
                .as('d')

            const revenueRegional = db
                .select({
                    name: regionalTerritory.regional,
                    target_rgb_all: sql<number>`CASE WHEN SUM(${regionalTargetRevenue.targetRgbAll}) IS NULL THEN '' ELSE ROUND(SUM(${regionalTargetRevenue.targetRgbAll}),0) END`.as('target_rgb_all'),
                    mtd_rgb: sql<number>`CASE WHEN SUM(${rgbRegional.mtdRgb}) IS NULL THEN '' ELSE ROUND(SUM(${rgbRegional.mtdRgb}),0) END`.as('mtd_rgb'),
                    mom_rgb: sql<string>`CASE WHEN SUM(${rgbRegional.mtdRgb}) IS NULL OR SUM(${rgbRegional.rgbLastMonth}) IS NULL OR SUM(${rgbRegional.rgbLastMonth}) = 0 THEN '' ELSE CONCAT(ROUND(((SUM(${rgbRegional.mtdRgb})/SUM(${rgbRegional.rgbLastMonth}))-1)*100,2),'%') END`.as('mom_rgb'),
                    yoy_rgb: sql<string>`CASE WHEN SUM(${rgbRegional.mtdRgb}) IS NULL OR SUM(${rgbRegional.rgbLastYear}) IS NULL OR SUM(${rgbRegional.rgbLastYear}) = 0 THEN '' ELSE CONCAT(ROUND(((SUM(${rgbRegional.mtdRgb})/SUM(${rgbRegional.rgbLastYear}))-1)*100,2),'%') END`.as('yoy_rgb'),
                    ach_target_fm_rgb: sql<string>`CASE WHEN SUM(${rgbRegional.mtdRgb}) IS NULL OR SUM(${regionalTargetRevenue.targetRgbAll}) IS NULL OR SUM(${regionalTargetRevenue.targetRgbAll}) = 0 THEN '' ELSE CONCAT(ROUND((SUM(${rgbRegional.mtdRgb})/SUM(${regionalTargetRevenue.targetRgbAll}))*100,2),'%') END`.as('ach_target_fm_rgb'),
                    drr_rgb: sql<string>`CASE WHEN SUM(${rgbRegional.mtdRgb}) IS NULL OR SUM(${regionalTargetRevenue.targetRgbAll}) IS NULL OR SUM(${regionalTargetRevenue.targetRgbAll}) = 0 THEN '' ELSE CONCAT(ROUND((SUM(${rgbRegional.mtdRgb})/(${today}/${daysInMonth}*(SUM(${regionalTargetRevenue.targetRgbAll}))))*100,2),'%') END`.as('drr_rgb'),
                    gap_to_target_rgb: sql<string>`CASE WHEN SUM(${rgbRegional.mtdRgb}) IS NULL OR SUM(${regionalTargetRevenue.targetRgbAll}) IS NULL THEN '' ELSE ROUND((SUM(${rgbRegional.mtdRgb}) - SUM(${regionalTargetRevenue.targetRgbAll})),0) END`.as('gap_to_target_rgb'),
                    penetration_to_cb: sql<string>`CASE WHEN SUM(${rgbRegional.mtdRgb}) IS NULL OR SUM(${rgbRegional.mtdCb}) IS NULL OR SUM(${rgbRegional.mtdCb}) = 0 THEN '' ELSE CONCAT(ROUND((SUM(${rgbRegional.mtdRgb})/SUM(${rgbRegional.mtdCb})*100),2),'%') END`.as('penetration_to_cb'),
                    mtd_rgb_data: sql<number>`CASE WHEN SUM(${rgbRegional.mtdRgbData}) IS NULL THEN '' ELSE ROUND(SUM(${rgbRegional.mtdRgbData}),0) END`.as('mtd_rgb_data'),
                    mom_rgb_data: sql<string>`CASE WHEN SUM(${rgbRegional.mtdRgbData}) IS NULL OR SUM(${rgbRegional.rgbDataLastMonth}) IS NULL OR SUM(${rgbRegional.rgbDataLastMonth}) = 0 THEN '' ELSE CONCAT(ROUND((((SUM(${rgbRegional.mtdRgbData})/SUM(${rgbRegional.rgbDataLastMonth}))-1)*100),2),'%') END`.as('mom_rgb_data'),
                    yoy_rgb_data: sql<string>`CASE WHEN SUM(${rgbRegional.mtdRgbData}) IS NULL OR SUM(${rgbRegional.rgbDataLastYear}) IS NULL OR SUM(${rgbRegional.rgbDataLastYear}) = 0 THEN '' ELSE CONCAT(ROUND((((SUM(${rgbRegional.mtdRgbData})/SUM(${rgbRegional.rgbDataLastYear}))-1)*100),2),'%') END`.as('yoy_rgb_data'),
                    mtd_rgb_digital: sql<number>`CASE WHEN SUM(${rgbRegional.mtdRgbDigital}) IS NULL THEN '' ELSE ROUND(SUM(${rgbRegional.mtdRgbDigital}),0) END`.as('mtd_rgb_digital'),
                    mom_rgb_digital: sql<string>`CASE WHEN SUM(${rgbRegional.mtdRgbDigital}) IS NULL OR SUM(${rgbRegional.rgbDigitalLastMonth}) IS NULL OR SUM(${rgbRegional.rgbDigitalLastMonth}) = 0 THEN '' ELSE CONCAT(ROUND((((SUM(${rgbRegional.mtdRgbDigital})/SUM(${rgbRegional.rgbDigitalLastMonth}))-1)*100),2),'%') END`.as('mom_rgb_digital'),
                    yoy_rgb_digital: sql<string>`CASE WHEN SUM(${rgbRegional.mtdRgbDigital}) IS NULL OR SUM(${rgbRegional.rgbDigitalLastYear}) IS NULL OR SUM(${rgbRegional.rgbDigitalLastYear}) = 0 THEN '' ELSE CONCAT(ROUND((((SUM(${rgbRegional.mtdRgbDigital})/SUM(${rgbRegional.rgbDigitalLastYear}))-1)*100),2),'%') END`.as('yoy_rgb_digital'),
                    mtd_arpu_data: sql<number>`CASE WHEN SUM(${summaryRevRegional.revBbM}) IS NULL OR SUM(${rgbRegional.mtdRgbData}) IS NULL OR SUM(${rgbRegional.mtdRgbData}) = 0 THEN '' ELSE ROUND(SUM(${summaryRevRegional.revBbM})/SUM(${rgbRegional.mtdRgbData}),0) END`.as('mtd_arpu_data'),
                    mom_arpu_data: sql<string>`CASE WHEN SUM(${summaryRevRegional.revBbM}) IS NULL OR SUM(${rgbRegional.mtdRgbData}) IS NULL OR SUM(${summaryRevRegional.revBbM1}) IS NULL OR SUM(${rgbRegional.rgbDataLastMonth}) IS NULL OR SUM(${rgbRegional.mtdRgbData}) = 0 OR SUM(${rgbRegional.rgbDataLastMonth}) = 0 THEN '' ELSE CONCAT(ROUND((((SUM(${summaryRevRegional.revBbM})/SUM(${rgbRegional.mtdRgbData}))/(SUM(${summaryRevRegional.revBbM1})/SUM(${rgbRegional.rgbDataLastMonth})))-1)*100,2),'%') END`.as('mom_arpu_data'),
                    yoy_arpu_data: sql<string>`CASE WHEN SUM(${summaryRevRegional.revBbM}) IS NULL OR SUM(${rgbRegional.mtdRgbData}) IS NULL OR SUM(${summaryRevRegional.revBbM12}) IS NULL OR SUM(${rgbRegional.rgbDataLastYear}) IS NULL OR SUM(${rgbRegional.mtdRgbData}) = 0 OR SUM(${rgbRegional.rgbDataLastYear}) = 0 THEN '' ELSE CONCAT(ROUND((((SUM(${summaryRevRegional.revBbM})/SUM(${rgbRegional.mtdRgbData}))/(SUM(${summaryRevRegional.revBbM12})/SUM(${rgbRegional.rgbDataLastYear})))-1)*100,2),'%') END`.as('yoy_arpu_data'),
                    mtd_arpu_digital: sql<number>`CASE WHEN SUM(${summaryRevRegional.revDigitalM}) IS NULL OR SUM(${rgbRegional.mtdRgbDigital}) IS NULL OR SUM(${rgbRegional.mtdRgbDigital}) = 0 THEN '' ELSE ROUND(SUM(${summaryRevRegional.revDigitalM})/SUM(${rgbRegional.mtdRgbDigital}),0) END`.as('mtd_arpu_digital'),
                    mom_arpu_digital: sql<string>`CASE WHEN SUM(${summaryRevRegional.revDigitalM}) IS NULL OR SUM(${rgbRegional.mtdRgbDigital}) IS NULL OR SUM(${summaryRevRegional.revDigitalM1}) IS NULL OR SUM(${rgbRegional.rgbDigitalLastMonth}) IS NULL OR SUM(${rgbRegional.mtdRgbDigital}) = 0 OR SUM(${rgbRegional.rgbDigitalLastMonth}) = 0 THEN '' ELSE CONCAT(ROUND((((SUM(${summaryRevRegional.revDigitalM})/SUM(${rgbRegional.mtdRgbDigital}))/(SUM(${summaryRevRegional.revDigitalM1})/SUM(${rgbRegional.rgbDigitalLastMonth})))-1)*100,2),'%') END`.as('mom_arpu_digital'),
                    yoy_arpu_digital: sql<string>`CASE WHEN SUM(${summaryRevRegional.revDigitalM}) IS NULL OR SUM(${rgbRegional.mtdRgbDigital}) IS NULL OR SUM(${summaryRevRegional.revDigitalM12}) IS NULL OR SUM(${rgbRegional.rgbDigitalLastYear}) IS NULL OR SUM(${rgbRegional.mtdRgbDigital}) = 0 OR SUM(${rgbRegional.rgbDigitalLastYear}) = 0 THEN '' ELSE CONCAT(ROUND((((SUM(${summaryRevRegional.revDigitalM})/SUM(${rgbRegional.mtdRgbDigital}))/(SUM(${summaryRevRegional.revDigitalM12})/SUM(${rgbRegional.rgbDigitalLastYear})))-1)*100,2),'%') END`.as('yoy_arpu_digital'),
                    mtd_rpmb: sql<number>`CASE WHEN SUM(${summaryRevRegional.revAllM}) IS NULL OR SUM(${rgbRegional.mtdPayload}) IS NULL OR SUM(${rgbRegional.mtdPayload}) = 0 THEN '' ELSE ROUND(SUM(${summaryRevRegional.revAllM})/(SUM(${rgbRegional.mtdPayload})/1024),2) END`.as('mtd_rpmb'),
                    mom_rpmb: sql<string>`CASE WHEN SUM(${summaryRevRegional.revAllM}) IS NULL OR SUM(${rgbRegional.mtdPayload}) IS NULL OR SUM(${summaryRevRegional.revAllM1}) IS NULL OR SUM(${rgbRegional.payloadLastMonth}) IS NULL OR SUM(${rgbRegional.mtdPayload}) = 0 OR SUM(${rgbRegional.payloadLastMonth}) = 0 THEN '' ELSE CONCAT(ROUND(((SUM(${summaryRevRegional.revAllM})/(SUM(${rgbRegional.mtdPayload})/1024)) / (SUM(${summaryRevRegional.revAllM1})/(SUM(${rgbRegional.payloadLastMonth})/1024))-1)*100,2),'%') END`.as('mom_rpmb'),
                    yoy_rpmb: sql<string>`CASE WHEN SUM(${summaryRevRegional.revAllM}) IS NULL OR SUM(${rgbRegional.mtdPayload}) IS NULL OR SUM(${summaryRevRegional.revAllM12}) IS NULL OR SUM(${rgbRegional.payloadLastYear}) IS NULL OR SUM(${rgbRegional.mtdPayload}) = 0 OR SUM(${rgbRegional.payloadLastYear}) = 0 THEN '' ELSE CONCAT(ROUND(((SUM(${summaryRevRegional.revAllM})/(SUM(${rgbRegional.mtdPayload})/1024)) / (SUM(${summaryRevRegional.revAllM12})/(SUM(${rgbRegional.payloadLastYear})/1024))-1)*100,2),'%') END`.as('yoy_rpmb'),
                })
                .from(regionalTerritory)
                .leftJoin(rgbRegional, eq(regionalTerritory.kabupaten, rgbRegional.kabupaten))
                .leftJoin(regionalTargetRevenue, eq(regionalTerritory.kabupaten, regionalTargetRevenue.territory))
                .leftJoin(summaryRevRegional, eq(regionalTerritory.kabupaten, summaryRevRegional.kabupaten))
                .groupBy(regionalTerritory.regional)

            const branchHeaderQuery = db
                .selectDistinct({
                    name: sql<string | null>`'BRANCH'`,
                    target_rgb_all: sql<number>`''`,
                    mtd_rgb: sql<number>`''`,
                    mom_rgb: sql<string>`''`,
                    yoy_rgb: sql<string>`''`,
                    ach_target_fm_rgb: sql<string>`''`,
                    drr_rgb: sql<string>`''`,
                    gap_to_target_rgb: sql<string>`''`,
                    penetration_to_cb: sql<string>`''`,

                    mtd_rgb_data: sql<number>`''`,
                    mom_rgb_data: sql<string>`''`,
                    yoy_rgb_data: sql<string>`''`,

                    mtd_rgb_digital: sql<number>`''`,
                    mom_rgb_digital: sql<string>`''`,
                    yoy_rgb_digital: sql<string>`''`,

                    mtd_arpu_data: sql<number>`''`,
                    mom_arpu_data: sql<string>`''`,
                    yoy_arpu_data: sql<string>`''`,

                    mtd_arpu_digital: sql<number>`''`,
                    mom_arpu_digital: sql<string>`''`,
                    yoy_arpu_digital: sql<string>`''`,

                    mtd_rpmb: sql<number>`''`,
                    mom_rpmb: sql<string>`''`,
                    yoy_rpmb: sql<string>`''`,
                })
                .from(feiTargetPuma)

            const revenueBranch = db
                .select({
                    name: regionalTerritory.branch,
                    target_rgb_all: sql<number>`ROUND(SUM(${regionalTargetRevenue.targetRgbAll}),0)`.as('target_rgb_all'),
                    mtd_rgb: sql<number>`ROUND(SUM(${rgbRegional.mtdRgb}),0)`.as('mtd_rgb'),
                    mom_rgb: sql<string>`CONCAT(ROUND(((SUM(${rgbRegional.mtdRgb})/SUM(${rgbRegional.rgbLastMonth}))-1)*100,2),'%')`.as('mom_rgb'),
                    yoy_rgb: sql<string>`CONCAT(ROUND(((SUM(${rgbRegional.mtdRgb})/SUM(${rgbRegional.rgbLastYear}))-1)*100,2),'%')`.as('yoy_rgb'),
                    ach_target_fm_rgb: sql<string>`CONCAT(ROUND((SUM(${rgbRegional.mtdRgb})/SUM(${regionalTargetRevenue.targetRgbAll}))*100,2),'%')`.as('ach_target_fm_rgb'),
                    drr_rgb: sql<string>`CONCAT(ROUND((SUM(${rgbRegional.mtdRgb})/(${today}/${daysInMonth}*(SUM(${regionalTargetRevenue.targetRgbAll}))))*100,2),'%')`.as('drr_rgb'),
                    gap_to_target_rgb: sql<string>`ROUND((COALESCE(SUM(${rgbRegional.mtdRgb}) - SUM(${regionalTargetRevenue.targetRgbAll}),0)),0)`.as('gap_to_target_rgb'),
                    penetration_to_cb: sql<string>`CONCAT(ROUND((SUM(${rgbRegional.mtdRgb})/SUM(${rgbRegional.mtdCb})*100),2),'%')`.as('penetration_to_cb'),

                    mtd_rgb_data: sql<number>`ROUND(SUM(${rgbRegional.mtdRgbData}),0)`.as('mtd_rgb_data'),
                    mom_rgb_data: sql<string>`CONCAT(ROUND((((SUM(${rgbRegional.mtdRgbData})/SUM(${rgbRegional.rgbDataLastMonth}))-1)*100),2),'%')`.as('mom_rgb_data'),
                    yoy_rgb_data: sql<string>`CONCAT(ROUND((((SUM(${rgbRegional.mtdRgbData})/SUM(${rgbRegional.rgbDataLastYear}))-1)*100),2),'%')`.as('yoy_rgb_data'),

                    mtd_rgb_digital: sql<number>`ROUND(SUM(${rgbRegional.mtdRgbDigital}),0)`.as('mtd_rgb_digital'),
                    mom_rgb_digital: sql<string>`CONCAT(ROUND((((SUM(${rgbRegional.mtdRgbDigital})/SUM(${rgbRegional.rgbDigitalLastMonth}))-1)*100),2),'%')`.as('mom_rgb_digital'),
                    yoy_rgb_digital: sql<string>`CONCAT(ROUND((((SUM(${rgbRegional.mtdRgbDigital})/SUM(${rgbRegional.rgbDigitalLastYear}))-1)*100),2),'%')`.as('yoy_rgb_digital'),

                    mtd_arpu_data: sql<number>`ROUND(SUM(${summaryRevRegional.revBbM})/SUM(${rgbRegional.mtdRgbData}),0)`.as('mtd_arpu_data'),
                    mom_arpu_data: sql<string>`CONCAT(ROUND((((SUM(${summaryRevRegional.revBbM})/SUM(${rgbRegional.mtdRgbData}))/(SUM(${summaryRevRegional.revBbM1})/SUM(${rgbRegional.rgbDataLastMonth})))-1)*100,2),'%')`.as('mom_arpu_data'),
                    yoy_arpu_data: sql<string>`CONCAT(ROUND((((SUM(${summaryRevRegional.revBbM})/SUM(${rgbRegional.mtdRgbData}))/(SUM(${summaryRevRegional.revBbM12})/SUM(${rgbRegional.rgbDataLastYear})))-1)*100,2),'%')`.as('yoy_arpu_data'),

                    mtd_arpu_digital: sql<number>`ROUND(SUM(${summaryRevRegional.revDigitalM})/SUM(${rgbRegional.mtdRgbDigital}),0)`.as('mtd_arpu_digital'),
                    mom_arpu_digital: sql<string>`CONCAT(ROUND((((SUM(${summaryRevRegional.revDigitalM})/SUM(${rgbRegional.mtdRgbDigital}))/(SUM(${summaryRevRegional.revDigitalM1})/SUM(${rgbRegional.rgbDigitalLastMonth})))-1)*100,2),'%')`.as('mom_arpu_digital'),
                    yoy_arpu_digital: sql<string>`CONCAT(ROUND((((SUM(${summaryRevRegional.revDigitalM})/SUM(${rgbRegional.mtdRgbDigital}))/(SUM(${summaryRevRegional.revDigitalM12})/SUM(${rgbRegional.rgbDigitalLastYear})))-1)*100,2),'%')`.as('yoy_arpu_digital'),

                    mtd_rpmb: sql<number>`ROUND(SUM(${summaryRevRegional.revAllM})/(SUM(${rgbRegional.mtdPayload})/1024),2)`.as('mtd_rpmb'),
                    mom_rpmb: sql<string>`CONCAT(ROUND(((SUM(${summaryRevRegional.revAllM})/(SUM(${rgbRegional.mtdPayload})/1024)) / (SUM(${summaryRevRegional.revAllM1})/(SUM(${rgbRegional.payloadLastMonth})/1024))-1)*100,2),'%')`.as('mom_rpmb'),
                    yoy_rpmb: sql<string>`CONCAT(ROUND(((SUM(${summaryRevRegional.revAllM})/(SUM(${rgbRegional.mtdPayload})/1024)) / (SUM(${summaryRevRegional.revAllM12})/(SUM(${rgbRegional.payloadLastYear})/1024))-1)*100,2),'%')`.as('yoy_rpmb'),
                })
                .from(regionalTerritory)
                .leftJoin(rgbRegional, eq(regionalTerritory.kabupaten, rgbRegional.kabupaten))
                .leftJoin(regionalTargetRevenue, eq(regionalTerritory.kabupaten, regionalTargetRevenue.territory))
                .leftJoin(summaryRevRegional, eq(regionalTerritory.kabupaten, summaryRevRegional.kabupaten))
                .groupBy(regionalTerritory.branch)

            const subbranchHeaderQuery = db
                .selectDistinct({
                    name: sql<string | null>`'SUBBRANCH'`,
                    target_rgb_all: sql<number>`''`,
                    mtd_rgb: sql<number>`''`,
                    mom_rgb: sql<string>`''`,
                    yoy_rgb: sql<string>`''`,
                    ach_target_fm_rgb: sql<string>`''`,
                    drr_rgb: sql<string>`''`,
                    gap_to_target_rgb: sql<string>`''`,
                    penetration_to_cb: sql<string>`''`,

                    mtd_rgb_data: sql<number>`''`,
                    mom_rgb_data: sql<string>`''`,
                    yoy_rgb_data: sql<string>`''`,

                    mtd_rgb_digital: sql<number>`''`,
                    mom_rgb_digital: sql<string>`''`,
                    yoy_rgb_digital: sql<string>`''`,

                    mtd_arpu_data: sql<number>`''`,
                    mom_arpu_data: sql<string>`''`,
                    yoy_arpu_data: sql<string>`''`,

                    mtd_arpu_digital: sql<number>`''`,
                    mom_arpu_digital: sql<string>`''`,
                    yoy_arpu_digital: sql<string>`''`,

                    mtd_rpmb: sql<number>`''`,
                    mom_rpmb: sql<string>`''`,
                    yoy_rpmb: sql<string>`''`,
                })
                .from(feiTargetPuma)

            const revenueSubbranch = db
                .select({
                    name: regionalTerritory.subbranch,
                    target_rgb_all: sql<number>`ROUND(SUM(${regionalTargetRevenue.targetRgbAll}),0)`.as('target_rgb_all'),
                    mtd_rgb: sql<number>`ROUND(SUM(${rgbRegional.mtdRgb}),0)`.as('mtd_rgb'),
                    mom_rgb: sql<string>`CONCAT(ROUND(((SUM(${rgbRegional.mtdRgb})/SUM(${rgbRegional.rgbLastMonth}))-1)*100,2),'%')`.as('mom_rgb'),
                    yoy_rgb: sql<string>`CONCAT(ROUND(((SUM(${rgbRegional.mtdRgb})/SUM(${rgbRegional.rgbLastYear}))-1)*100,2),'%')`.as('yoy_rgb'),
                    ach_target_fm_rgb: sql<string>`CONCAT(ROUND((SUM(${rgbRegional.mtdRgb})/SUM(${regionalTargetRevenue.targetRgbAll}))*100,2),'%')`.as('ach_target_fm_rgb'),
                    drr_rgb: sql<string>`CONCAT(ROUND((SUM(${rgbRegional.mtdRgb})/(${today}/${daysInMonth}*(SUM(${regionalTargetRevenue.targetRgbAll}))))*100,2),'%')`.as('drr_rgb'),
                    gap_to_target_rgb: sql<string>`ROUND((COALESCE(SUM(${rgbRegional.mtdRgb}) - SUM(${regionalTargetRevenue.targetRgbAll}),0)),0)`.as('gap_to_target_rgb'),
                    penetration_to_cb: sql<string>`CONCAT(ROUND((SUM(${rgbRegional.mtdRgb})/SUM(${rgbRegional.mtdCb})*100),2),'%')`.as('penetration_to_cb'),

                    mtd_rgb_data: sql<number>`ROUND(SUM(${rgbRegional.mtdRgbData}),0)`.as('mtd_rgb_data'),
                    mom_rgb_data: sql<string>`CONCAT(ROUND((((SUM(${rgbRegional.mtdRgbData})/SUM(${rgbRegional.rgbDataLastMonth}))-1)*100),2),'%')`.as('mom_rgb_data'),
                    yoy_rgb_data: sql<string>`CONCAT(ROUND((((SUM(${rgbRegional.mtdRgbData})/SUM(${rgbRegional.rgbDataLastYear}))-1)*100),2),'%')`.as('yoy_rgb_data'),

                    mtd_rgb_digital: sql<number>`ROUND(SUM(${rgbRegional.mtdRgbDigital}),0)`.as('mtd_rgb_digital'),
                    mom_rgb_digital: sql<string>`CONCAT(ROUND((((SUM(${rgbRegional.mtdRgbDigital})/SUM(${rgbRegional.rgbDigitalLastMonth}))-1)*100),2),'%')`.as('mom_rgb_digital'),
                    yoy_rgb_digital: sql<string>`CONCAT(ROUND((((SUM(${rgbRegional.mtdRgbDigital})/SUM(${rgbRegional.rgbDigitalLastYear}))-1)*100),2),'%')`.as('yoy_rgb_digital'),

                    mtd_arpu_data: sql<number>`ROUND(SUM(${summaryRevRegional.revBbM})/SUM(${rgbRegional.mtdRgbData}),0)`.as('mtd_arpu_data'),
                    mom_arpu_data: sql<string>`CONCAT(ROUND((((SUM(${summaryRevRegional.revBbM})/SUM(${rgbRegional.mtdRgbData}))/(SUM(${summaryRevRegional.revBbM1})/SUM(${rgbRegional.rgbDataLastMonth})))-1)*100,2),'%')`.as('mom_arpu_data'),
                    yoy_arpu_data: sql<string>`CONCAT(ROUND((((SUM(${summaryRevRegional.revBbM})/SUM(${rgbRegional.mtdRgbData}))/(SUM(${summaryRevRegional.revBbM12})/SUM(${rgbRegional.rgbDataLastYear})))-1)*100,2),'%')`.as('yoy_arpu_data'),

                    mtd_arpu_digital: sql<number>`ROUND(SUM(${summaryRevRegional.revDigitalM})/SUM(${rgbRegional.mtdRgbDigital}),0)`.as('mtd_arpu_digital'),
                    mom_arpu_digital: sql<string>`CONCAT(ROUND((((SUM(${summaryRevRegional.revDigitalM})/SUM(${rgbRegional.mtdRgbDigital}))/(SUM(${summaryRevRegional.revDigitalM1})/SUM(${rgbRegional.rgbDigitalLastMonth})))-1)*100,2),'%')`.as('mom_arpu_digital'),
                    yoy_arpu_digital: sql<string>`CONCAT(ROUND((((SUM(${summaryRevRegional.revDigitalM})/SUM(${rgbRegional.mtdRgbDigital}))/(SUM(${summaryRevRegional.revDigitalM12})/SUM(${rgbRegional.rgbDigitalLastYear})))-1)*100,2),'%')`.as('yoy_arpu_digital'),

                    mtd_rpmb: sql<number>`ROUND(SUM(${summaryRevRegional.revAllM})/(SUM(${rgbRegional.mtdPayload})/1024),2)`.as('mtd_rpmb'),
                    mom_rpmb: sql<string>`CONCAT(ROUND(((SUM(${summaryRevRegional.revAllM})/(SUM(${rgbRegional.mtdPayload})/1024)) / (SUM(${summaryRevRegional.revAllM1})/(SUM(${rgbRegional.payloadLastMonth})/1024))-1)*100,2),'%')`.as('mom_rpmb'),
                    yoy_rpmb: sql<string>`CONCAT(ROUND(((SUM(${summaryRevRegional.revAllM})/(SUM(${rgbRegional.mtdPayload})/1024)) / (SUM(${summaryRevRegional.revAllM12})/(SUM(${rgbRegional.payloadLastYear})/1024))-1)*100,2),'%')`.as('yoy_rpmb'),
                })
                .from(regionalTerritory)
                .leftJoin(rgbRegional, eq(regionalTerritory.kabupaten, rgbRegional.kabupaten))
                .leftJoin(regionalTargetRevenue, eq(regionalTerritory.kabupaten, regionalTargetRevenue.territory))
                .leftJoin(summaryRevRegional, eq(regionalTerritory.kabupaten, summaryRevRegional.kabupaten))
                .groupBy(regionalTerritory.subbranch)

            const clusterHeaderQuery = db
                .selectDistinct({
                    name: sql<string | null>`'CLUSTER'`,
                    target_rgb_all: sql<number>`''`,
                    mtd_rgb: sql<number>`''`,
                    mom_rgb: sql<string>`''`,
                    yoy_rgb: sql<string>`''`,
                    ach_target_fm_rgb: sql<string>`''`,
                    drr_rgb: sql<string>`''`,
                    gap_to_target_rgb: sql<string>`''`,
                    penetration_to_cb: sql<string>`''`,

                    mtd_rgb_data: sql<number>`''`,
                    mom_rgb_data: sql<string>`''`,
                    yoy_rgb_data: sql<string>`''`,

                    mtd_rgb_digital: sql<number>`''`,
                    mom_rgb_digital: sql<string>`''`,
                    yoy_rgb_digital: sql<string>`''`,

                    mtd_arpu_data: sql<number>`''`,
                    mom_arpu_data: sql<string>`''`,
                    yoy_arpu_data: sql<string>`''`,

                    mtd_arpu_digital: sql<number>`''`,
                    mom_arpu_digital: sql<string>`''`,
                    yoy_arpu_digital: sql<string>`''`,

                    mtd_rpmb: sql<number>`''`,
                    mom_rpmb: sql<string>`''`,
                    yoy_rpmb: sql<string>`''`,
                })
                .from(feiTargetPuma)

            const revenueCluster = db
                .select({
                    name: regionalTerritory.cluster,
                    target_rgb_all: sql<number>`ROUND(SUM(${regionalTargetRevenue.targetRgbAll}),0)`.as('target_rgb_all'),
                    mtd_rgb: sql<number>`ROUND(SUM(${rgbRegional.mtdRgb}),0)`.as('mtd_rgb'),
                    mom_rgb: sql<string>`CONCAT(ROUND(((SUM(${rgbRegional.mtdRgb})/SUM(${rgbRegional.rgbLastMonth}))-1)*100,2),'%')`.as('mom_rgb'),
                    yoy_rgb: sql<string>`CONCAT(ROUND(((SUM(${rgbRegional.mtdRgb})/SUM(${rgbRegional.rgbLastYear}))-1)*100,2),'%')`.as('yoy_rgb'),
                    ach_target_fm_rgb: sql<string>`CONCAT(ROUND((SUM(${rgbRegional.mtdRgb})/SUM(${regionalTargetRevenue.targetRgbAll}))*100,2),'%')`.as('ach_target_fm_rgb'),
                    drr_rgb: sql<string>`CONCAT(ROUND((SUM(${rgbRegional.mtdRgb})/(${today}/${daysInMonth}*(SUM(${regionalTargetRevenue.targetRgbAll}))))*100,2),'%')`.as('drr_rgb'),
                    gap_to_target_rgb: sql<string>`ROUND((COALESCE(SUM(${rgbRegional.mtdRgb}) - SUM(${regionalTargetRevenue.targetRgbAll}),0)),0)`.as('gap_to_target_rgb'),
                    penetration_to_cb: sql<string>`CONCAT(ROUND((SUM(${rgbRegional.mtdRgb})/SUM(${rgbRegional.mtdCb})*100),2),'%')`.as('penetration_to_cb'),

                    mtd_rgb_data: sql<number>`ROUND(SUM(${rgbRegional.mtdRgbData}),0)`.as('mtd_rgb_data'),
                    mom_rgb_data: sql<string>`CONCAT(ROUND((((SUM(${rgbRegional.mtdRgbData})/SUM(${rgbRegional.rgbDataLastMonth}))-1)*100),2),'%')`.as('mom_rgb_data'),
                    yoy_rgb_data: sql<string>`CONCAT(ROUND((((SUM(${rgbRegional.mtdRgbData})/SUM(${rgbRegional.rgbDataLastYear}))-1)*100),2),'%')`.as('yoy_rgb_data'),

                    mtd_rgb_digital: sql<number>`ROUND(SUM(${rgbRegional.mtdRgbDigital}),0)`.as('mtd_rgb_digital'),
                    mom_rgb_digital: sql<string>`CONCAT(ROUND((((SUM(${rgbRegional.mtdRgbDigital})/SUM(${rgbRegional.rgbDigitalLastMonth}))-1)*100),2),'%')`.as('mom_rgb_digital'),
                    yoy_rgb_digital: sql<string>`CONCAT(ROUND((((SUM(${rgbRegional.mtdRgbDigital})/SUM(${rgbRegional.rgbDigitalLastYear}))-1)*100),2),'%')`.as('yoy_rgb_digital'),

                    mtd_arpu_data: sql<number>`ROUND(SUM(${summaryRevRegional.revBbM})/SUM(${rgbRegional.mtdRgbData}),0)`.as('mtd_arpu_data'),
                    mom_arpu_data: sql<string>`CONCAT(ROUND((((SUM(${summaryRevRegional.revBbM})/SUM(${rgbRegional.mtdRgbData}))/(SUM(${summaryRevRegional.revBbM1})/SUM(${rgbRegional.rgbDataLastMonth})))-1)*100,2),'%')`.as('mom_arpu_data'),
                    yoy_arpu_data: sql<string>`CONCAT(ROUND((((SUM(${summaryRevRegional.revBbM})/SUM(${rgbRegional.mtdRgbData}))/(SUM(${summaryRevRegional.revBbM12})/SUM(${rgbRegional.rgbDataLastYear})))-1)*100,2),'%')`.as('yoy_arpu_data'),

                    mtd_arpu_digital: sql<number>`ROUND(SUM(${summaryRevRegional.revDigitalM})/SUM(${rgbRegional.mtdRgbDigital}),0)`.as('mtd_arpu_digital'),
                    mom_arpu_digital: sql<string>`CONCAT(ROUND((((SUM(${summaryRevRegional.revDigitalM})/SUM(${rgbRegional.mtdRgbDigital}))/(SUM(${summaryRevRegional.revDigitalM1})/SUM(${rgbRegional.rgbDigitalLastMonth})))-1)*100,2),'%')`.as('mom_arpu_digital'),
                    yoy_arpu_digital: sql<string>`CONCAT(ROUND((((SUM(${summaryRevRegional.revDigitalM})/SUM(${rgbRegional.mtdRgbDigital}))/(SUM(${summaryRevRegional.revDigitalM12})/SUM(${rgbRegional.rgbDigitalLastYear})))-1)*100,2),'%')`.as('yoy_arpu_digital'),

                    mtd_rpmb: sql<number>`ROUND(SUM(${summaryRevRegional.revAllM})/(SUM(${rgbRegional.mtdPayload})/1024),2)`.as('mtd_rpmb'),
                    mom_rpmb: sql<string>`CONCAT(ROUND(((SUM(${summaryRevRegional.revAllM})/(SUM(${rgbRegional.mtdPayload})/1024)) / (SUM(${summaryRevRegional.revAllM1})/(SUM(${rgbRegional.payloadLastMonth})/1024))-1)*100,2),'%')`.as('mom_rpmb'),
                    yoy_rpmb: sql<string>`CONCAT(ROUND(((SUM(${summaryRevRegional.revAllM})/(SUM(${rgbRegional.mtdPayload})/1024)) / (SUM(${summaryRevRegional.revAllM12})/(SUM(${rgbRegional.payloadLastYear})/1024))-1)*100,2),'%')`.as('yoy_rpmb'),
                })
                .from(regionalTerritory)
                .leftJoin(rgbRegional, eq(regionalTerritory.kabupaten, rgbRegional.kabupaten))
                .leftJoin(regionalTargetRevenue, eq(regionalTerritory.kabupaten, regionalTargetRevenue.territory))
                .leftJoin(summaryRevRegional, eq(regionalTerritory.kabupaten, summaryRevRegional.kabupaten))
                .groupBy(regionalTerritory.cluster)

            const kabupatenHeaderQuery = db
                .selectDistinct({
                    name: sql<string | null>`'KABUPATEN'`,
                    target_rgb_all: sql<number>`''`,
                    mtd_rgb: sql<number>`''`,
                    mom_rgb: sql<string>`''`,
                    yoy_rgb: sql<string>`''`,
                    ach_target_fm_rgb: sql<string>`''`,
                    drr_rgb: sql<string>`''`,
                    gap_to_target_rgb: sql<string>`''`,
                    penetration_to_cb: sql<string>`''`,

                    mtd_rgb_data: sql<number>`''`,
                    mom_rgb_data: sql<string>`''`,
                    yoy_rgb_data: sql<string>`''`,

                    mtd_rgb_digital: sql<number>`''`,
                    mom_rgb_digital: sql<string>`''`,
                    yoy_rgb_digital: sql<string>`''`,

                    mtd_arpu_data: sql<number>`''`,
                    mom_arpu_data: sql<string>`''`,
                    yoy_arpu_data: sql<string>`''`,

                    mtd_arpu_digital: sql<number>`''`,
                    mom_arpu_digital: sql<string>`''`,
                    yoy_arpu_digital: sql<string>`''`,

                    mtd_rpmb: sql<number>`''`,
                    mom_rpmb: sql<string>`''`,
                    yoy_rpmb: sql<string>`''`,
                })
                .from(feiTargetPuma)

            const revenueKabupaten = db
                .select({
                    name: regionalTerritory.kabupaten,
                    target_rgb_all: sql<number>`ROUND(SUM(${regionalTargetRevenue.targetRgbAll}),0)`.as('target_rgb_all'),
                    mtd_rgb: sql<number>`ROUND(SUM(${rgbRegional.mtdRgb}),0)`.as('mtd_rgb'),
                    mom_rgb: sql<string>`CONCAT(ROUND(((SUM(${rgbRegional.mtdRgb})/SUM(${rgbRegional.rgbLastMonth}))-1)*100,2),'%')`.as('mom_rgb'),
                    yoy_rgb: sql<string>`CONCAT(ROUND(((SUM(${rgbRegional.mtdRgb})/SUM(${rgbRegional.rgbLastYear}))-1)*100,2),'%')`.as('yoy_rgb'),
                    ach_target_fm_rgb: sql<string>`CONCAT(ROUND((SUM(${rgbRegional.mtdRgb})/SUM(${regionalTargetRevenue.targetRgbAll}))*100,2),'%')`.as('ach_target_fm_rgb'),
                    drr_rgb: sql<string>`CONCAT(ROUND((SUM(${rgbRegional.mtdRgb})/(${today}/${daysInMonth}*(SUM(${regionalTargetRevenue.targetRgbAll}))))*100,2),'%')`.as('drr_rgb'),
                    gap_to_target_rgb: sql<string>`ROUND((COALESCE(SUM(${rgbRegional.mtdRgb}) - SUM(${regionalTargetRevenue.targetRgbAll}),0)),0)`.as('gap_to_target_rgb'),
                    penetration_to_cb: sql<string>`CONCAT(ROUND((SUM(${rgbRegional.mtdRgb})/SUM(${rgbRegional.mtdCb})*100),2),'%')`.as('penetration_to_cb'),

                    mtd_rgb_data: sql<number>`ROUND(SUM(${rgbRegional.mtdRgbData}),0)`.as('mtd_rgb_data'),
                    mom_rgb_data: sql<string>`CONCAT(ROUND((((SUM(${rgbRegional.mtdRgbData})/SUM(${rgbRegional.rgbDataLastMonth}))-1)*100),2),'%')`.as('mom_rgb_data'),
                    yoy_rgb_data: sql<string>`CONCAT(ROUND((((SUM(${rgbRegional.mtdRgbData})/SUM(${rgbRegional.rgbDataLastYear}))-1)*100),2),'%')`.as('yoy_rgb_data'),

                    mtd_rgb_digital: sql<number>`ROUND(SUM(${rgbRegional.mtdRgbDigital}),0)`.as('mtd_rgb_digital'),
                    mom_rgb_digital: sql<string>`CONCAT(ROUND((((SUM(${rgbRegional.mtdRgbDigital})/SUM(${rgbRegional.rgbDigitalLastMonth}))-1)*100),2),'%')`.as('mom_rgb_digital'),
                    yoy_rgb_digital: sql<string>`CONCAT(ROUND((((SUM(${rgbRegional.mtdRgbDigital})/SUM(${rgbRegional.rgbDigitalLastYear}))-1)*100),2),'%')`.as('yoy_rgb_digital'),

                    mtd_arpu_data: sql<number>`ROUND(SUM(${summaryRevRegional.revBbM})/SUM(${rgbRegional.mtdRgbData}),0)`.as('mtd_arpu_data'),
                    mom_arpu_data: sql<string>`CONCAT(ROUND((((SUM(${summaryRevRegional.revBbM})/SUM(${rgbRegional.mtdRgbData}))/(SUM(${summaryRevRegional.revBbM1})/SUM(${rgbRegional.rgbDataLastMonth})))-1)*100,2),'%')`.as('mom_arpu_data'),
                    yoy_arpu_data: sql<string>`CONCAT(ROUND((((SUM(${summaryRevRegional.revBbM})/SUM(${rgbRegional.mtdRgbData}))/(SUM(${summaryRevRegional.revBbM12})/SUM(${rgbRegional.rgbDataLastYear})))-1)*100,2),'%')`.as('yoy_arpu_data'),

                    mtd_arpu_digital: sql<number>`ROUND(SUM(${summaryRevRegional.revDigitalM})/SUM(${rgbRegional.mtdRgbDigital}),0)`.as('mtd_arpu_digital'),
                    mom_arpu_digital: sql<string>`CONCAT(ROUND((((SUM(${summaryRevRegional.revDigitalM})/SUM(${rgbRegional.mtdRgbDigital}))/(SUM(${summaryRevRegional.revDigitalM1})/SUM(${rgbRegional.rgbDigitalLastMonth})))-1)*100,2),'%')`.as('mom_arpu_digital'),
                    yoy_arpu_digital: sql<string>`CONCAT(ROUND((((SUM(${summaryRevRegional.revDigitalM})/SUM(${rgbRegional.mtdRgbDigital}))/(SUM(${summaryRevRegional.revDigitalM12})/SUM(${rgbRegional.rgbDigitalLastYear})))-1)*100,2),'%')`.as('yoy_arpu_digital'),

                    mtd_rpmb: sql<number>`ROUND(SUM(${summaryRevRegional.revAllM})/(SUM(${rgbRegional.mtdPayload})/1024),2)`.as('mtd_rpmb'),
                    mom_rpmb: sql<string>`CONCAT(ROUND(((SUM(${summaryRevRegional.revAllM})/(SUM(${rgbRegional.mtdPayload})/1024)) / (SUM(${summaryRevRegional.revAllM1})/(SUM(${rgbRegional.payloadLastMonth})/1024))-1)*100,2),'%')`.as('mom_rpmb'),
                    yoy_rpmb: sql<string>`CONCAT(ROUND(((SUM(${summaryRevRegional.revAllM})/(SUM(${rgbRegional.mtdPayload})/1024)) / (SUM(${summaryRevRegional.revAllM12})/(SUM(${rgbRegional.payloadLastYear})/1024))-1)*100,2),'%')`.as('yoy_rpmb'),
                })
                .from(regionalTerritory)
                .leftJoin(rgbRegional, eq(regionalTerritory.kabupaten, rgbRegional.kabupaten))
                .leftJoin(regionalTargetRevenue, eq(regionalTerritory.kabupaten, regionalTargetRevenue.territory))
                .leftJoin(summaryRevRegional, eq(regionalTerritory.kabupaten, summaryRevRegional.kabupaten))
                .groupBy(regionalTerritory.kabupaten)

            const [finalDataRevenue] = await Promise.all([
                unionAll(revenueRegional, branchHeaderQuery, revenueBranch, subbranchHeaderQuery, revenueSubbranch, clusterHeaderQuery, revenueCluster, kabupatenHeaderQuery, revenueKabupaten)
            ])

            return c.json({ data: finalDataRevenue }, 200)
        })
    .get('/paying-subs', zValidator('query', z.object({ date: z.string().optional(), branch: z.string().optional(), subbranch: z.string().optional(), cluster: z.string().optional(), kabupaten: z.string().optional() })),
        async c => {
            const { date, branch, subbranch, cluster, kabupaten } = c.req.valid('query')
            const selectedDate = date ? new Date(date) : subDays(new Date(), 2)

            const lastDayOfSelectedMonth = endOfMonth(selectedDate);
            const isEndOfMonth = selectedDate.getDate() === lastDayOfSelectedMonth.getDate();

            const endOfCurrMonth = isEndOfMonth ? lastDayOfSelectedMonth : selectedDate;
            const endOfPrevMonth = isEndOfMonth ? endOfMonth(subMonths(selectedDate, 1)) : subMonths(selectedDate, 1);
            const endOfPrevYearSameMonth = isEndOfMonth ? endOfMonth(subYears(selectedDate, 1)) : subYears(selectedDate, 1);

            const currDate = format(selectedDate, 'yyyy-MM-dd')
            const prevDate = format(endOfPrevMonth, 'yyyy-MM-dd');
            const prevYearCurrDate = format(endOfPrevYearSameMonth, 'yyyy-MM-dd');
            const yyyyMM = format(selectedDate, 'yyyyMM')
            const daysInMonth = getDaysInMonth(selectedDate)
            const today = Number(format(selectedDate, 'd'))

            const regionalTerritory = db
                .select({
                    regional: territoryArea4.regional,
                })
                .from(territoryArea4)
                .where(eq(territoryArea4.regional, 'PUMA'))
                .groupBy(territoryArea4.regional)
                .as('a')

            const rgbRegional = db
                .select({
                    regional: summaryRgbHqRegional.regional,
                    cbLastYear: summaryRgbHqRegional.cb_m12,
                    cbLastMonth: summaryRgbHqRegional.cb_m1,
                    mtdCb: summaryRgbHqRegional.cb_mtd,
                    rgbLastYear: summaryRgbHqRegional.rgb_m12,
                    rgbLastMonth: summaryRgbHqRegional.rgb_m1,
                    mtdRgb: summaryRgbHqRegional.rgb_mtd,
                    rgbDataLastYear: summaryRgbHqRegional.rgb_data_m12,
                    rgbDataLastMonth: summaryRgbHqRegional.rgb_data_m1,
                    mtdRgbData: summaryRgbHqRegional.rgb_data_mtd,
                    rgbDigitalLastYear: summaryRgbHqRegional.rgb_digital_m12,
                    rgbDigitalLastMonth: summaryRgbHqRegional.rgb_digital_m1,
                    mtdRgbDigital: summaryRgbHqRegional.rgb_digital_mtd,
                    payloadLastYear: summaryRgbHqRegional.payload_user_m12,
                    payloadLastMonth: summaryRgbHqRegional.payload_user_m1,
                    mtdPayload: summaryRgbHqRegional.payload_user_mtd
                })
                .from(summaryRgbHqRegional)
                .where(and(
                    eq(summaryRgbHqRegional.event_date, currDate),
                    eq(summaryRgbHqRegional.regional, 'PUMA'),
                    eq(summaryRgbHqRegional.catlos, 'all'),
                    eq(summaryRgbHqRegional.brand, 'all')
                ))
                .groupBy(summaryRgbHqRegional.regional)
                .as('b')

            const regionalTargetRevenue = db
                .select({
                    regional: territoryArea4.regional,
                    target_paying_subs: sum(feiTargetPuma.paying_subs).as('target_paying_subs')
                })
                .from(feiTargetPuma)
                .rightJoin(territoryArea4, eq(feiTargetPuma.territory, territoryArea4.kabupaten))
                .where(eq(feiTargetPuma.periode, yyyyMM))
                .groupBy(territoryArea4.regional)
                .as('c')

            const summaryRevRegional = db
                .select({
                    regional: summaryRevAllRegional.regional,
                    revAllM: summaryRevAllRegional.rev_all_m,
                    revAllM1: sql<number>`SUM(CASE WHEN ${summaryRevAllRegional.tgl} = ${prevDate} THEN ${summaryRevAllRegional.rev_all_m} END)`.as('rev_all_m1'),
                    revAllM12: sql<number>`SUM(CASE WHEN ${summaryRevAllRegional.tgl} = ${prevYearCurrDate} THEN ${summaryRevAllRegional.rev_all_m} END)`.as('rev_all_m12'),

                    revBbM: summaryRevAllRegional.rev_bb_m,
                    revBbM1: sql<number>`SUM(CASE WHEN ${summaryRevAllRegional.tgl} = ${prevDate} THEN ${summaryRevAllRegional.rev_bb_m} END)`.as('rev_bb_m1'),
                    revBbM12: sql<number>`SUM(CASE WHEN ${summaryRevAllRegional.tgl} = ${prevYearCurrDate} THEN ${summaryRevAllRegional.rev_bb_m} END)`.as('rev_bb_m12'),

                    revVoiceM: summaryRevAllRegional.rev_voice_m,
                    revVoiceM1: sql<number>`SUM(CASE WHEN ${summaryRevAllRegional.tgl} = ${prevDate} THEN ${summaryRevAllRegional.rev_voice_m} END)`.as('rev_voice_m1'),
                    revVoiceM12: sql<number>`SUM(CASE WHEN ${summaryRevAllRegional.tgl} = ${prevYearCurrDate} THEN ${summaryRevAllRegional.rev_voice_m} END)`.as('rev_voice_m12'),

                    revDigitalM: summaryRevAllRegional.rev_digital_m,
                    revDigitalM1: sql<number>`SUM(CASE WHEN ${summaryRevAllRegional.tgl} = ${prevDate} THEN ${summaryRevAllRegional.rev_digital_m} END)`.as('rev_digital_m1'),
                    revDigitalM12: sql<number>`SUM(CASE WHEN ${summaryRevAllRegional.tgl} = ${prevYearCurrDate} THEN ${summaryRevAllRegional.rev_digital_m} END)`.as('rev_digital_m12')
                })
                .from(summaryRevAllRegional)
                .where(and(
                    eq(summaryRevAllRegional.regional, 'PUMA'),
                    eq(summaryRevAllRegional.newAbcStrate, 'all')
                ))
                .groupBy(summaryRevAllRegional.regional)
                .as('d')

            const revenueRegional = db
                .select({
                    name: regionalTerritory.regional,
                    target_rgb_all: sql<number>`ROUND(SUM(${regionalTargetRevenue.target_paying_subs}),0)`.as('target_rgb_all'),
                    mtd_rgb: sql<number>`ROUND(SUM(${rgbRegional.mtdRgb}),0)`.as('mtd_rgb'),
                    mom_rgb: sql<string>`CONCAT(ROUND(((SUM(${rgbRegional.mtdRgb})/SUM(${rgbRegional.rgbLastMonth}))-1)*100,2),'%')`.as('mom_rgb'),
                    yoy_rgb: sql<string>`CONCAT(ROUND(((SUM(${rgbRegional.mtdRgb})/SUM(${rgbRegional.rgbLastYear}))-1)*100,2),'%')`.as('yoy_rgb'),

                    ach_target_fm_rgb: sql<string>`CONCAT(ROUND((SUM(${rgbRegional.mtdRgb})/SUM(${regionalTargetRevenue.target_paying_subs}))*100,2),'%')`.as('ach_target_fm_rgb'),
                    drr_rgb: sql<string>`CONCAT(ROUND((SUM(${rgbRegional.mtdRgb})/(${today}/${daysInMonth}*(SUM(${regionalTargetRevenue.target_paying_subs}))))*100,2),'%')`.as('drr_rgb'),
                    gap_to_target_rgb: sql<string>`ROUND((COALESCE(SUM(${rgbRegional.mtdRgb}) - SUM(${regionalTargetRevenue.target_paying_subs}),0)),0)`.as('gap_to_target_rgb'),
                    penetration_to_cb: sql<string>`CONCAT(ROUND((SUM(${rgbRegional.mtdRgb})/SUM(${rgbRegional.mtdCb})*100),2),'%')`.as('penetration_to_cb'),

                    mtd_rgb_data: sql<number>`ROUND(SUM(${rgbRegional.mtdRgbData}),0)`.as('mtd_rgb_data'),
                    mom_rgb_data: sql<string>`CONCAT(ROUND((((SUM(${rgbRegional.mtdRgbData})/SUM(${rgbRegional.rgbDataLastMonth}))-1)*100),2),'%')`.as('mom_rgb_data'),
                    yoy_rgb_data: sql<string>`CONCAT(ROUND((((SUM(${rgbRegional.mtdRgbData})/SUM(${rgbRegional.rgbDataLastYear}))-1)*100),2),'%')`.as('yoy_rgb_data'),

                    mtd_rgb_digital: sql<number>`ROUND(SUM(${rgbRegional.mtdRgbDigital}),0)`.as('mtd_rgb_digital'),
                    mom_rgb_digital: sql<string>`CONCAT(ROUND((((SUM(${rgbRegional.mtdRgbDigital})/SUM(${rgbRegional.rgbDigitalLastMonth}))-1)*100),2),'%')`.as('mom_rgb_digital'),
                    yoy_rgb_digital: sql<string>`CONCAT(ROUND((((SUM(${rgbRegional.mtdRgbDigital})/SUM(${rgbRegional.rgbDigitalLastYear}))-1)*100),2),'%')`.as('yoy_rgb_digital'),

                    mtd_arpu: sql<number>`ROUND(SUM(${summaryRevRegional.revAllM})/SUM(${rgbRegional.mtdRgb}),0)`.as('mtd_arpu'),
                    mom_arpu: sql<string>`CONCAT(ROUND((((SUM(${summaryRevRegional.revAllM})/SUM(${rgbRegional.mtdRgb}))/(SUM(${summaryRevRegional.revAllM1})/SUM(${rgbRegional.rgbDataLastMonth})))-1)*100,2),'%')`.as('mom_arpu'),
                    yoy_arpu: sql<string>`CONCAT(ROUND((((SUM(${summaryRevRegional.revAllM})/SUM(${rgbRegional.mtdRgb}))/(SUM(${summaryRevRegional.revAllM12})/SUM(${rgbRegional.rgbDataLastYear})))-1)*100,2),'%')`.as('yoy_arpu'),

                    mtd_arpu_data: sql<number>`ROUND(SUM(${summaryRevRegional.revBbM})/SUM(${rgbRegional.mtdRgbData}),0)`.as('mtd_arpu_data'),
                    mom_arpu_data: sql<string>`CONCAT(ROUND((((SUM(${summaryRevRegional.revBbM})/SUM(${rgbRegional.mtdRgbData}))/(SUM(${summaryRevRegional.revBbM1})/SUM(${rgbRegional.rgbDataLastMonth})))-1)*100,2),'%')`.as('mom_arpu_data'),
                    yoy_arpu_data: sql<string>`CONCAT(ROUND((((SUM(${summaryRevRegional.revBbM})/SUM(${rgbRegional.mtdRgbData}))/(SUM(${summaryRevRegional.revBbM12})/SUM(${rgbRegional.rgbDataLastYear})))-1)*100,2),'%')`.as('yoy_arpu_data'),

                    mtd_arpu_digital: sql<number>`ROUND(SUM(${summaryRevRegional.revDigitalM})/SUM(${rgbRegional.mtdRgbDigital}),0)`.as('mtd_arpu_digital'),
                    mom_arpu_digital: sql<string>`CONCAT(ROUND((((SUM(${summaryRevRegional.revDigitalM})/SUM(${rgbRegional.mtdRgbDigital}))/(SUM(${summaryRevRegional.revDigitalM1})/SUM(${rgbRegional.rgbDigitalLastMonth})))-1)*100,2),'%')`.as('mom_arpu_digital'),
                    yoy_arpu_digital: sql<string>`CONCAT(ROUND((((SUM(${summaryRevRegional.revDigitalM})/SUM(${rgbRegional.mtdRgbDigital}))/(SUM(${summaryRevRegional.revDigitalM12})/SUM(${rgbRegional.rgbDigitalLastYear})))-1)*100,2),'%')`.as('yoy_arpu_digital'),

                    mtd_rpmb: sql<number>`ROUND(SUM(${summaryRevRegional.revAllM})/(SUM(${rgbRegional.mtdPayload})/1024),2)`.as('mtd_rpmb'),
                    mom_rpmb: sql<string>`CONCAT(ROUND(((SUM(${summaryRevRegional.revAllM})/(SUM(${rgbRegional.mtdPayload})/1024)) / (SUM(${summaryRevRegional.revAllM1})/(SUM(${rgbRegional.payloadLastMonth})/1024))-1)*100,2),'%')`.as('mom_rpmb'),
                    yoy_rpmb: sql<string>`CONCAT(ROUND(((SUM(${summaryRevRegional.revAllM})/(SUM(${rgbRegional.mtdPayload})/1024)) / (SUM(${summaryRevRegional.revAllM12})/(SUM(${rgbRegional.payloadLastYear})/1024))-1)*100,2),'%')`.as('yoy_rpmb'),
                })
                .from(regionalTerritory)
                .leftJoin(rgbRegional, eq(regionalTerritory.regional, rgbRegional.regional))
                .leftJoin(regionalTargetRevenue, eq(regionalTerritory.regional, regionalTargetRevenue.regional))
                .leftJoin(summaryRevRegional, eq(regionalTerritory.regional, summaryRevRegional.regional))
                .groupBy(regionalTerritory.regional)

            const branchHeaderQuery = db
                .selectDistinct({
                    name: sql<string | null>`'BRANCH'`,
                    target_rgb_all: sql<number>`''`,
                    mtd_rgb: sql<number>`''`,
                    mom_rgb: sql<string>`''`,
                    yoy_rgb: sql<string>`''`,
                    ach_target_fm_rgb: sql<string>`''`,
                    drr_rgb: sql<string>`''`,
                    gap_to_target_rgb: sql<string>`''`,
                    penetration_to_cb: sql<string>`''`,

                    mtd_rgb_data: sql<number>`''`,
                    mom_rgb_data: sql<string>`''`,
                    yoy_rgb_data: sql<string>`''`,

                    mtd_rgb_digital: sql<number>`''`,
                    mom_rgb_digital: sql<string>`''`,
                    yoy_rgb_digital: sql<string>`''`,

                    mtd_arpu: sql<number>`''`,
                    mom_arpu: sql<string>`''`,
                    yoy_arpu: sql<string>`''`,

                    mtd_arpu_data: sql<number>`''`,
                    mom_arpu_data: sql<string>`''`,
                    yoy_arpu_data: sql<string>`''`,

                    mtd_arpu_digital: sql<number>`''`,
                    mom_arpu_digital: sql<string>`''`,
                    yoy_arpu_digital: sql<string>`''`,

                    mtd_rpmb: sql<number>`''`,
                    mom_rpmb: sql<string>`''`,
                    yoy_rpmb: sql<string>`''`,
                })
                .from(feiTargetPuma)

            const branchTerritory = db
                .select({
                    branch: territoryArea4.branch,
                })
                .from(territoryArea4)
                .where(and(
                    eq(territoryArea4.regional, 'PUMA'),
                    branch ? eq(territoryArea4.branch, branch) : undefined
                ))
                .groupBy(territoryArea4.branch)
                .as('a')

            const rgbBranch = db
                .select({
                    branch: summaryRgbHqBranch.branch,
                    cbLastYear: summaryRgbHqBranch.cb_m12,
                    cbLastMonth: summaryRgbHqBranch.cb_m1,
                    mtdCb: summaryRgbHqBranch.cb_mtd,
                    rgbLastYear: summaryRgbHqBranch.rgb_m12,
                    rgbLastMonth: summaryRgbHqBranch.rgb_m1,
                    mtdRgb: summaryRgbHqBranch.rgb_mtd,
                    rgbDataLastYear: summaryRgbHqBranch.rgb_data_m12,
                    rgbDataLastMonth: summaryRgbHqBranch.rgb_data_m1,
                    mtdRgbData: summaryRgbHqBranch.rgb_data_mtd,
                    rgbDigitalLastYear: summaryRgbHqBranch.rgb_digital_m12,
                    rgbDigitalLastMonth: summaryRgbHqBranch.rgb_digital_m1,
                    mtdRgbDigital: summaryRgbHqBranch.rgb_digital_mtd,
                    payloadLastYear: summaryRgbHqBranch.payload_user_m12,
                    payloadLastMonth: summaryRgbHqBranch.payload_user_m1,
                    mtdPayload: summaryRgbHqBranch.payload_user_mtd
                })
                .from(summaryRgbHqBranch)
                .where(and(
                    eq(summaryRgbHqBranch.event_date, currDate),
                    eq(summaryRgbHqBranch.regional, 'PUMA'),
                    eq(summaryRgbHqBranch.catlos, 'all'),
                    eq(summaryRgbHqBranch.brand, 'all')
                ))
                .groupBy(summaryRgbHqBranch.branch)
                .as('b')

            const branchTargetRevenue = db
                .select({
                    branch: territoryArea4.branch,
                    target_paying_subs: sum(feiTargetPuma.paying_subs).as('target_paying_subs')
                })
                .from(feiTargetPuma)
                .rightJoin(territoryArea4, eq(feiTargetPuma.territory, territoryArea4.kabupaten))
                .where(eq(feiTargetPuma.periode, yyyyMM))
                .groupBy(territoryArea4.branch)
                .as('c')

            const summaryRevBranch = db
                .select({
                    branch: summaryRevAllBranch.branch,
                    revAllM: summaryRevAllBranch.rev_all_m,
                    revAllM1: sql<number>`SUM(CASE WHEN ${summaryRevAllBranch.tgl} = ${prevDate} THEN ${summaryRevAllBranch.rev_all_m} END)`.as('rev_all_m1'),
                    revAllM12: sql<number>`SUM(CASE WHEN ${summaryRevAllBranch.tgl} = ${prevYearCurrDate} THEN ${summaryRevAllBranch.rev_all_m} END)`.as('rev_all_m12'),

                    revBbM: summaryRevAllBranch.rev_bb_m,
                    revBbM1: sql<number>`SUM(CASE WHEN ${summaryRevAllBranch.tgl} = ${prevDate} THEN ${summaryRevAllBranch.rev_bb_m} END)`.as('rev_bb_m1'),
                    revBbM12: sql<number>`SUM(CASE WHEN ${summaryRevAllBranch.tgl} = ${prevYearCurrDate} THEN ${summaryRevAllBranch.rev_bb_m} END)`.as('rev_bb_m12'),

                    revVoiceM: summaryRevAllBranch.rev_voice_m,
                    revVoiceM1: sql<number>`SUM(CASE WHEN ${summaryRevAllBranch.tgl} = ${prevDate} THEN ${summaryRevAllBranch.rev_voice_m} END)`.as('rev_voice_m1'),
                    revVoiceM12: sql<number>`SUM(CASE WHEN ${summaryRevAllBranch.tgl} = ${prevYearCurrDate} THEN ${summaryRevAllBranch.rev_voice_m} END)`.as('rev_voice_m12'),

                    revDigitalM: summaryRevAllBranch.rev_digital_m,
                    revDigitalM1: sql<number>`SUM(CASE WHEN ${summaryRevAllBranch.tgl} = ${prevDate} THEN ${summaryRevAllBranch.rev_digital_m} END)`.as('rev_digital_m1'),
                    revDigitalM12: sql<number>`SUM(CASE WHEN ${summaryRevAllBranch.tgl} = ${prevYearCurrDate} THEN ${summaryRevAllBranch.rev_digital_m} END)`.as('rev_digital_m12')
                })
                .from(summaryRevAllBranch)
                .where(and(
                    eq(summaryRevAllBranch.regional, 'PUMA'),
                    eq(summaryRevAllBranch.newAbcStrate, 'all')
                ))
                .groupBy(summaryRevAllBranch.branch)
                .as('d')

            const revenueBranch = db
                .select({
                    name: branchTerritory.branch,
                    target_rgb_all: sql<number>`ROUND(SUM(${branchTargetRevenue.target_paying_subs}),0)`.as('target_rgb_all'),
                    mtd_rgb: sql<number>`ROUND(SUM(${rgbBranch.mtdRgb}),0)`.as('mtd_rgb'),
                    mom_rgb: sql<string>`CONCAT(ROUND(((SUM(${rgbBranch.mtdRgb})/SUM(${rgbBranch.rgbLastMonth}))-1)*100,2),'%')`.as('mom_rgb'),
                    yoy_rgb: sql<string>`CONCAT(ROUND(((SUM(${rgbBranch.mtdRgb})/SUM(${rgbBranch.rgbLastYear}))-1)*100,2),'%')`.as('yoy_rgb'),

                    ach_target_fm_rgb: sql<string>`CONCAT(ROUND((SUM(${rgbBranch.mtdRgb})/SUM(${branchTargetRevenue.target_paying_subs}))*100,2),'%')`.as('ach_target_fm_rgb'),
                    drr_rgb: sql<string>`CONCAT(ROUND((SUM(${rgbBranch.mtdRgb})/(${today}/${daysInMonth}*(SUM(${branchTargetRevenue.target_paying_subs}))))*100,2),'%')`.as('drr_rgb'),
                    gap_to_target_rgb: sql<string>`ROUND((COALESCE(SUM(${rgbBranch.mtdRgb}) - SUM(${branchTargetRevenue.target_paying_subs}),0)),0)`.as('gap_to_target_rgb'),
                    penetration_to_cb: sql<string>`CONCAT(ROUND((SUM(${rgbBranch.mtdRgb})/SUM(${rgbBranch.mtdCb})*100),2),'%')`.as('penetration_to_cb'),

                    mtd_rgb_data: sql<number>`ROUND(SUM(${rgbBranch.mtdRgbData}),0)`.as('mtd_rgb_data'),
                    mom_rgb_data: sql<string>`CONCAT(ROUND((((SUM(${rgbBranch.mtdRgbData})/SUM(${rgbBranch.rgbDataLastMonth}))-1)*100),2),'%')`.as('mom_rgb_data'),
                    yoy_rgb_data: sql<string>`CONCAT(ROUND((((SUM(${rgbBranch.mtdRgbData})/SUM(${rgbBranch.rgbDataLastYear}))-1)*100),2),'%')`.as('yoy_rgb_data'),

                    mtd_rgb_digital: sql<number>`ROUND(SUM(${rgbBranch.mtdRgbDigital}),0)`.as('mtd_rgb_digital'),
                    mom_rgb_digital: sql<string>`CONCAT(ROUND((((SUM(${rgbBranch.mtdRgbDigital})/SUM(${rgbBranch.rgbDigitalLastMonth}))-1)*100),2),'%')`.as('mom_rgb_digital'),
                    yoy_rgb_digital: sql<string>`CONCAT(ROUND((((SUM(${rgbBranch.mtdRgbDigital})/SUM(${rgbBranch.rgbDigitalLastYear}))-1)*100),2),'%')`.as('yoy_rgb_digital'),

                    mtd_arpu: sql<number>`ROUND(SUM(${summaryRevBranch.revAllM})/SUM(${rgbBranch.mtdRgb}),0)`.as('mtd_arpu'),
                    mom_arpu: sql<string>`CONCAT(ROUND((((SUM(${summaryRevBranch.revAllM})/SUM(${rgbBranch.mtdRgb}))/(SUM(${summaryRevBranch.revAllM1})/SUM(${rgbBranch.rgbDataLastMonth})))-1)*100,2),'%')`.as('mom_arpu'),
                    yoy_arpu: sql<string>`CONCAT(ROUND((((SUM(${summaryRevBranch.revAllM})/SUM(${rgbBranch.mtdRgb}))/(SUM(${summaryRevBranch.revAllM12})/SUM(${rgbBranch.rgbDataLastYear})))-1)*100,2),'%')`.as('yoy_arpu'),

                    mtd_arpu_data: sql<number>`ROUND(SUM(${summaryRevBranch.revBbM})/SUM(${rgbBranch.mtdRgbData}),0)`.as('mtd_arpu_data'),
                    mom_arpu_data: sql<string>`CONCAT(ROUND((((SUM(${summaryRevBranch.revBbM})/SUM(${rgbBranch.mtdRgbData}))/(SUM(${summaryRevBranch.revBbM1})/SUM(${rgbBranch.rgbDataLastMonth})))-1)*100,2),'%')`.as('mom_arpu_data'),
                    yoy_arpu_data: sql<string>`CONCAT(ROUND((((SUM(${summaryRevBranch.revBbM})/SUM(${rgbBranch.mtdRgbData}))/(SUM(${summaryRevBranch.revBbM12})/SUM(${rgbBranch.rgbDataLastYear})))-1)*100,2),'%')`.as('yoy_arpu_data'),

                    mtd_arpu_digital: sql<number>`ROUND(SUM(${summaryRevBranch.revDigitalM})/SUM(${rgbBranch.mtdRgbDigital}),0)`.as('mtd_arpu_digital'),
                    mom_arpu_digital: sql<string>`CONCAT(ROUND((((SUM(${summaryRevBranch.revDigitalM})/SUM(${rgbBranch.mtdRgbDigital}))/(SUM(${summaryRevBranch.revDigitalM1})/SUM(${rgbBranch.rgbDigitalLastMonth})))-1)*100,2),'%')`.as('mom_arpu_digital'),
                    yoy_arpu_digital: sql<string>`CONCAT(ROUND((((SUM(${summaryRevBranch.revDigitalM})/SUM(${rgbBranch.mtdRgbDigital}))/(SUM(${summaryRevBranch.revDigitalM12})/SUM(${rgbBranch.rgbDigitalLastYear})))-1)*100,2),'%')`.as('yoy_arpu_digital'),

                    mtd_rpmb: sql<number>`ROUND(SUM(${summaryRevBranch.revAllM})/(SUM(${rgbBranch.mtdPayload})/1024),2)`.as('mtd_rpmb'),
                    mom_rpmb: sql<string>`CONCAT(ROUND(((SUM(${summaryRevBranch.revAllM})/(SUM(${rgbBranch.mtdPayload})/1024)) / (SUM(${summaryRevBranch.revAllM1})/(SUM(${rgbBranch.payloadLastMonth})/1024))-1)*100,2),'%')`.as('mom_rpmb'),
                    yoy_rpmb: sql<string>`CONCAT(ROUND(((SUM(${summaryRevBranch.revAllM})/(SUM(${rgbBranch.mtdPayload})/1024)) / (SUM(${summaryRevBranch.revAllM12})/(SUM(${rgbBranch.payloadLastYear})/1024))-1)*100,2),'%')`.as('yoy_rpmb'),
                })
                .from(branchTerritory)
                .leftJoin(rgbBranch, eq(branchTerritory.branch, rgbBranch.branch))
                .leftJoin(branchTargetRevenue, eq(branchTerritory.branch, branchTargetRevenue.branch))
                .leftJoin(summaryRevBranch, eq(branchTerritory.branch, summaryRevBranch.branch))
                .groupBy(branchTerritory.branch)

            const subbranchHeaderQuery = db
                .selectDistinct({
                    name: sql<string | null>`'SUBBRANCH'`,
                    target_rgb_all: sql<number>`''`,
                    mtd_rgb: sql<number>`''`,
                    mom_rgb: sql<string>`''`,
                    yoy_rgb: sql<string>`''`,
                    ach_target_fm_rgb: sql<string>`''`,
                    drr_rgb: sql<string>`''`,
                    gap_to_target_rgb: sql<string>`''`,
                    penetration_to_cb: sql<string>`''`,

                    mtd_rgb_data: sql<number>`''`,
                    mom_rgb_data: sql<string>`''`,
                    yoy_rgb_data: sql<string>`''`,

                    mtd_rgb_digital: sql<number>`''`,
                    mom_rgb_digital: sql<string>`''`,
                    yoy_rgb_digital: sql<string>`''`,

                    mtd_arpu: sql<number>`''`,
                    mom_arpu: sql<string>`''`,
                    yoy_arpu: sql<string>`''`,

                    mtd_arpu_data: sql<number>`''`,
                    mom_arpu_data: sql<string>`''`,
                    yoy_arpu_data: sql<string>`''`,

                    mtd_arpu_digital: sql<number>`''`,
                    mom_arpu_digital: sql<string>`''`,
                    yoy_arpu_digital: sql<string>`''`,

                    mtd_rpmb: sql<number>`''`,
                    mom_rpmb: sql<string>`''`,
                    yoy_rpmb: sql<string>`''`,
                })
                .from(feiTargetPuma)

            const subbranchTerritory = db
                .select({
                    subbranch: territoryArea4.subbranch,
                })
                .from(territoryArea4)
                .where(branch && subbranch ? and(
                    eq(territoryArea4.regional, 'PUMA'),
                    eq(territoryArea4.branch, branch),
                    eq(territoryArea4.subbranch, subbranch)
                ) : branch ? and(
                    eq(territoryArea4.regional, 'PUMA'),
                    eq(territoryArea4.branch, branch)
                ) : eq(territoryArea4.regional, 'PUMA'))
                .groupBy(territoryArea4.subbranch)
                .as('a')

            const rgbSubbranch = db
                .select({
                    subbranch: sql<string>`
                    CASE
                        WHEN kabupaten IN ('AMBON','KOTA AMBON','MALUKU TENGAH','SERAM BAGIAN TIMUR') THEN 'AMBON'
                        WHEN kabupaten IN ('KEPULAUAN ARU','KOTA TUAL','MALUKU BARAT DAYA','MALUKU TENGGARA','MALUKU TENGGARA BARAT','KEPULAUAN TANIMBAR') THEN 'KEPULAUAN AMBON'
                        WHEN kabupaten IN ('BURU', 'BURU SELATAN', 'SERAM BAGIAN BARAT') THEN 'SERAM BURU'
                        WHEN kabupaten IN ('KOTA JAYAPURA') THEN 'JAYAPURA'
                        WHEN kabupaten IN ('JAYAPURA','KEEROM','MAMBERAMO RAYA','SARMI','BIAK','BIAK NUMFOR','KEPULAUAN YAPEN','SUPIORI','WAROPEN','JAYAWIJAYA','LANNY JAYA','MAMBERAMO TENGAH','NDUGA','PEGUNUNGAN BINTANG','TOLIKARA','YAHUKIMO','YALIMO') THEN 'SENTANI'
                        WHEN kabupaten IN ('MANOKWARI') THEN 'MANOKWARI'
                        WHEN kabupaten IN ('FAKFAK','FAK FAK','KAIMANA','MANOKWARI SELATAN','PEGUNUNGAN ARFAK','TELUK BINTUNI','TELUK WONDAMA') THEN 'MANOKWARI OUTER'
                        WHEN kabupaten IN ('KOTA SORONG','MAYBRAT','RAJA AMPAT','SORONG','SORONG SELATAN','TAMBRAUW') THEN 'SORONG RAJA AMPAT'
                        WHEN kabupaten IN ('ASMAT', 'BOVEN DIGOEL', 'MAPPI', 'MERAUKE') THEN 'MERAUKE'
                        WHEN kabupaten IN ('INTAN JAYA','MIMIKA','PUNCAK','PUNCAK JAYA','TIMIKA') THEN 'MIMIKA'
                        WHEN kabupaten IN ('DEIYAI', 'DOGIYAI', 'NABIRE', 'PANIAI') THEN 'NABIRE'
                        ELSE NULL
                    END
                    `.as('subbranch'),
                    cbLastYear: summaryRgbHqKabupaten.cb_m12,
                    cbLastMonth: summaryRgbHqKabupaten.cb_m1,
                    mtdCb: summaryRgbHqKabupaten.cb_mtd,
                    rgbLastYear: summaryRgbHqKabupaten.rgb_m12,
                    rgbLastMonth: summaryRgbHqKabupaten.rgb_m1,
                    mtdRgb: summaryRgbHqKabupaten.rgb_mtd,
                    rgbDataLastYear: summaryRgbHqKabupaten.rgb_data_m12,
                    rgbDataLastMonth: summaryRgbHqKabupaten.rgb_data_m1,
                    mtdRgbData: summaryRgbHqKabupaten.rgb_data_mtd,
                    rgbDigitalLastYear: summaryRgbHqKabupaten.rgb_digital_m12,
                    rgbDigitalLastMonth: summaryRgbHqKabupaten.rgb_digital_m1,
                    mtdRgbDigital: summaryRgbHqKabupaten.rgb_digital_mtd,
                    payloadLastYear: summaryRgbHqKabupaten.payload_user_m12,
                    payloadLastMonth: summaryRgbHqKabupaten.payload_user_m1,
                    mtdPayload: summaryRgbHqKabupaten.payload_user_mtd
                })
                .from(summaryRgbHqKabupaten)
                .where(and(
                    eq(summaryRgbHqKabupaten.event_date, currDate),
                    eq(summaryRgbHqKabupaten.regional, 'PUMA'),
                    eq(summaryRgbHqKabupaten.catlos, 'all'),
                    eq(summaryRgbHqKabupaten.brand, 'all')
                ))
                .groupBy(sql`1`)
                .as('b')

            const subbranchTargetRevenue = db
                .select({
                    subbranch: territoryArea4.subbranch,
                    target_paying_subs: sum(feiTargetPuma.paying_subs).as('target_paying_subs')
                })
                .from(feiTargetPuma)
                .rightJoin(territoryArea4, eq(feiTargetPuma.territory, territoryArea4.kabupaten))
                .where(eq(feiTargetPuma.periode, yyyyMM))
                .groupBy(territoryArea4.subbranch)
                .as('c')

            const summaryRevSubbranch = db
                .select({
                    subbranch: summaryRevAllSubbranch.subbranch,
                    revAllM: summaryRevAllSubbranch.rev_all_m,
                    revAllM1: sql<number>`SUM(CASE WHEN ${summaryRevAllSubbranch.tgl} = ${prevDate} THEN ${summaryRevAllSubbranch.rev_all_m} END)`.as('rev_all_m1'),
                    revAllM12: sql<number>`SUM(CASE WHEN ${summaryRevAllSubbranch.tgl} = ${prevYearCurrDate} THEN ${summaryRevAllSubbranch.rev_all_m} END)`.as('rev_all_m12'),

                    revBbM: summaryRevAllSubbranch.rev_bb_m,
                    revBbM1: sql<number>`SUM(CASE WHEN ${summaryRevAllSubbranch.tgl} = ${prevDate} THEN ${summaryRevAllSubbranch.rev_bb_m} END)`.as('rev_bb_m1'),
                    revBbM12: sql<number>`SUM(CASE WHEN ${summaryRevAllSubbranch.tgl} = ${prevYearCurrDate} THEN ${summaryRevAllSubbranch.rev_bb_m} END)`.as('rev_bb_m12'),

                    revVoiceM: summaryRevAllSubbranch.rev_voice_m,
                    revVoiceM1: sql<number>`SUM(CASE WHEN ${summaryRevAllSubbranch.tgl} = ${prevDate} THEN ${summaryRevAllSubbranch.rev_voice_m} END)`.as('rev_voice_m1'),
                    revVoiceM12: sql<number>`SUM(CASE WHEN ${summaryRevAllSubbranch.tgl} = ${prevYearCurrDate} THEN ${summaryRevAllSubbranch.rev_voice_m} END)`.as('rev_voice_m12'),

                    revDigitalM: summaryRevAllSubbranch.rev_digital_m,
                    revDigitalM1: sql<number>`SUM(CASE WHEN ${summaryRevAllSubbranch.tgl} = ${prevDate} THEN ${summaryRevAllSubbranch.rev_digital_m} END)`.as('rev_digital_m1'),
                    revDigitalM12: sql<number>`SUM(CASE WHEN ${summaryRevAllSubbranch.tgl} = ${prevYearCurrDate} THEN ${summaryRevAllSubbranch.rev_digital_m} END)`.as('rev_digital_m12')
                })
                .from(summaryRevAllSubbranch)
                .where(and(
                    eq(summaryRevAllSubbranch.regional, 'PUMA'),
                    eq(summaryRevAllSubbranch.newAbcStrate, 'all')
                ))
                .groupBy(summaryRevAllSubbranch.branch)
                .as('d')

            const revenueSubbranch = db
                .select({
                    name: subbranchTerritory.subbranch,
                    target_rgb_all: sql<number>`ROUND(SUM(${subbranchTargetRevenue.target_paying_subs}),0)`.as('target_rgb_all'),
                    mtd_rgb: sql<number>`ROUND(SUM(${rgbSubbranch.mtdRgb}),0)`.as('mtd_rgb'),
                    mom_rgb: sql<string>`CONCAT(ROUND(((SUM(${rgbSubbranch.mtdRgb})/SUM(${rgbSubbranch.rgbLastMonth}))-1)*100,2),'%')`.as('mom_rgb'),
                    yoy_rgb: sql<string>`CONCAT(ROUND(((SUM(${rgbSubbranch.mtdRgb})/SUM(${rgbSubbranch.rgbLastYear}))-1)*100,2),'%')`.as('yoy_rgb'),

                    ach_target_fm_rgb: sql<string>`CONCAT(ROUND((SUM(${rgbSubbranch.mtdRgb})/SUM(${subbranchTargetRevenue.target_paying_subs}))*100,2),'%')`.as('ach_target_fm_rgb'),
                    drr_rgb: sql<string>`CONCAT(ROUND((SUM(${rgbSubbranch.mtdRgb})/(${today}/${daysInMonth}*(SUM(${subbranchTargetRevenue.target_paying_subs}))))*100,2),'%')`.as('drr_rgb'),
                    gap_to_target_rgb: sql<string>`ROUND((COALESCE(SUM(${rgbSubbranch.mtdRgb}) - SUM(${subbranchTargetRevenue.target_paying_subs}),0)),0)`.as('gap_to_target_rgb'),
                    penetration_to_cb: sql<string>`CONCAT(ROUND((SUM(${rgbSubbranch.mtdRgb})/SUM(${rgbSubbranch.mtdCb})*100),2),'%')`.as('penetration_to_cb'),

                    mtd_rgb_data: sql<number>`ROUND(SUM(${rgbSubbranch.mtdRgbData}),0)`.as('mtd_rgb_data'),
                    mom_rgb_data: sql<string>`CONCAT(ROUND((((SUM(${rgbSubbranch.mtdRgbData})/SUM(${rgbSubbranch.rgbDataLastMonth}))-1)*100),2),'%')`.as('mom_rgb_data'),
                    yoy_rgb_data: sql<string>`CONCAT(ROUND((((SUM(${rgbSubbranch.mtdRgbData})/SUM(${rgbSubbranch.rgbDataLastYear}))-1)*100),2),'%')`.as('yoy_rgb_data'),

                    mtd_rgb_digital: sql<number>`ROUND(SUM(${rgbSubbranch.mtdRgbDigital}),0)`.as('mtd_rgb_digital'),
                    mom_rgb_digital: sql<string>`CONCAT(ROUND((((SUM(${rgbSubbranch.mtdRgbDigital})/SUM(${rgbSubbranch.rgbDigitalLastMonth}))-1)*100),2),'%')`.as('mom_rgb_digital'),
                    yoy_rgb_digital: sql<string>`CONCAT(ROUND((((SUM(${rgbSubbranch.mtdRgbDigital})/SUM(${rgbSubbranch.rgbDigitalLastYear}))-1)*100),2),'%')`.as('yoy_rgb_digital'),

                    mtd_arpu: sql<number>`ROUND(SUM(${summaryRevSubbranch.revAllM})/SUM(${rgbSubbranch.mtdRgb}),0)`.as('mtd_arpu'),
                    mom_arpu: sql<string>`CONCAT(ROUND((((SUM(${summaryRevSubbranch.revAllM})/SUM(${rgbSubbranch.mtdRgb}))/(SUM(${summaryRevSubbranch.revAllM1})/SUM(${rgbSubbranch.rgbDataLastMonth})))-1)*100,2),'%')`.as('mom_arpu'),
                    yoy_arpu: sql<string>`CONCAT(ROUND((((SUM(${summaryRevSubbranch.revAllM})/SUM(${rgbSubbranch.mtdRgb}))/(SUM(${summaryRevSubbranch.revAllM12})/SUM(${rgbSubbranch.rgbDataLastYear})))-1)*100,2),'%')`.as('yoy_arpu'),

                    mtd_arpu_data: sql<number>`ROUND(SUM(${summaryRevSubbranch.revBbM})/SUM(${rgbSubbranch.mtdRgbData}),0)`.as('mtd_arpu_data'),
                    mom_arpu_data: sql<string>`CONCAT(ROUND((((SUM(${summaryRevSubbranch.revBbM})/SUM(${rgbSubbranch.mtdRgbData}))/(SUM(${summaryRevSubbranch.revBbM1})/SUM(${rgbSubbranch.rgbDataLastMonth})))-1)*100,2),'%')`.as('mom_arpu_data'),
                    yoy_arpu_data: sql<string>`CONCAT(ROUND((((SUM(${summaryRevSubbranch.revBbM})/SUM(${rgbSubbranch.mtdRgbData}))/(SUM(${summaryRevSubbranch.revBbM12})/SUM(${rgbSubbranch.rgbDataLastYear})))-1)*100,2),'%')`.as('yoy_arpu_data'),

                    mtd_arpu_digital: sql<number>`ROUND(SUM(${summaryRevSubbranch.revDigitalM})/SUM(${rgbSubbranch.mtdRgbDigital}),0)`.as('mtd_arpu_digital'),
                    mom_arpu_digital: sql<string>`CONCAT(ROUND((((SUM(${summaryRevSubbranch.revDigitalM})/SUM(${rgbSubbranch.mtdRgbDigital}))/(SUM(${summaryRevSubbranch.revDigitalM1})/SUM(${rgbSubbranch.rgbDigitalLastMonth})))-1)*100,2),'%')`.as('mom_arpu_digital'),
                    yoy_arpu_digital: sql<string>`CONCAT(ROUND((((SUM(${summaryRevSubbranch.revDigitalM})/SUM(${rgbSubbranch.mtdRgbDigital}))/(SUM(${summaryRevSubbranch.revDigitalM12})/SUM(${rgbSubbranch.rgbDigitalLastYear})))-1)*100,2),'%')`.as('yoy_arpu_digital'),

                    mtd_rpmb: sql<number>`ROUND(SUM(${summaryRevSubbranch.revAllM})/(SUM(${rgbSubbranch.mtdPayload})/1024),2)`.as('mtd_rpmb'),
                    mom_rpmb: sql<string>`CONCAT(ROUND(((SUM(${summaryRevSubbranch.revAllM})/(SUM(${rgbSubbranch.mtdPayload})/1024)) / (SUM(${summaryRevSubbranch.revAllM1})/(SUM(${rgbSubbranch.payloadLastMonth})/1024))-1)*100,2),'%')`.as('mom_rpmb'),
                    yoy_rpmb: sql<string>`CONCAT(ROUND(((SUM(${summaryRevSubbranch.revAllM})/(SUM(${rgbSubbranch.mtdPayload})/1024)) / (SUM(${summaryRevSubbranch.revAllM12})/(SUM(${rgbSubbranch.payloadLastYear})/1024))-1)*100,2),'%')`.as('yoy_rpmb'),
                })
                .from(subbranchTerritory)
                .leftJoin(rgbSubbranch, eq(subbranchTerritory.subbranch, sql`b.subbranch`))
                .leftJoin(subbranchTargetRevenue, eq(subbranchTerritory.subbranch, subbranchTargetRevenue.subbranch))
                .leftJoin(summaryRevSubbranch, eq(subbranchTerritory.subbranch, summaryRevSubbranch.subbranch))
                .groupBy(subbranchTerritory.subbranch)

            const clusterHeaderQuery = db
                .selectDistinct({
                    name: sql<string | null>`'CLUSTER'`,
                    target_rgb_all: sql<number>`''`,
                    mtd_rgb: sql<number>`''`,
                    mom_rgb: sql<string>`''`,
                    yoy_rgb: sql<string>`''`,
                    ach_target_fm_rgb: sql<string>`''`,
                    drr_rgb: sql<string>`''`,
                    gap_to_target_rgb: sql<string>`''`,
                    penetration_to_cb: sql<string>`''`,

                    mtd_rgb_data: sql<number>`''`,
                    mom_rgb_data: sql<string>`''`,
                    yoy_rgb_data: sql<string>`''`,

                    mtd_rgb_digital: sql<number>`''`,
                    mom_rgb_digital: sql<string>`''`,
                    yoy_rgb_digital: sql<string>`''`,

                    mtd_arpu: sql<number>`''`,
                    mom_arpu: sql<string>`''`,
                    yoy_arpu: sql<string>`''`,

                    mtd_arpu_data: sql<number>`''`,
                    mom_arpu_data: sql<string>`''`,
                    yoy_arpu_data: sql<string>`''`,

                    mtd_arpu_digital: sql<number>`''`,
                    mom_arpu_digital: sql<string>`''`,
                    yoy_arpu_digital: sql<string>`''`,

                    mtd_rpmb: sql<number>`''`,
                    mom_rpmb: sql<string>`''`,
                    yoy_rpmb: sql<string>`''`,
                })
                .from(feiTargetPuma)

            const clusterTerritory = db
                .select({
                    cluster: territoryArea4.cluster,
                })
                .from(territoryArea4)
                .where(branch && subbranch && cluster ? and(
                    eq(territoryArea4.regional, 'PUMA'),
                    eq(territoryArea4.branch, branch),
                    eq(territoryArea4.subbranch, subbranch),
                    eq(territoryArea4.cluster, cluster),
                ) : branch && subbranch ? and(
                    eq(territoryArea4.regional, 'PUMA'),
                    eq(territoryArea4.branch, branch),
                    eq(territoryArea4.subbranch, subbranch),
                ) : branch ? and(
                    eq(territoryArea4.regional, 'PUMA'),
                    eq(territoryArea4.branch, branch),
                ) : eq(territoryArea4.regional, 'PUMA'))
                .groupBy(territoryArea4.cluster)
                .as('a')

            const rgbCluster = db
                .select({
                    cluster: summaryRgbHqCluster.cluster,
                    cbLastYear: summaryRgbHqCluster.cb_m12,
                    cbLastMonth: summaryRgbHqCluster.cb_m1,
                    mtdCb: summaryRgbHqCluster.cb_mtd,
                    rgbLastYear: summaryRgbHqCluster.rgb_m12,
                    rgbLastMonth: summaryRgbHqCluster.rgb_m1,
                    mtdRgb: summaryRgbHqCluster.rgb_mtd,
                    rgbDataLastYear: summaryRgbHqCluster.rgb_data_m12,
                    rgbDataLastMonth: summaryRgbHqCluster.rgb_data_m1,
                    mtdRgbData: summaryRgbHqCluster.rgb_data_mtd,
                    rgbDigitalLastYear: summaryRgbHqCluster.rgb_digital_m12,
                    rgbDigitalLastMonth: summaryRgbHqCluster.rgb_digital_m1,
                    mtdRgbDigital: summaryRgbHqCluster.rgb_digital_mtd,
                    payloadLastYear: summaryRgbHqCluster.payload_user_m12,
                    payloadLastMonth: summaryRgbHqCluster.payload_user_m1,
                    mtdPayload: summaryRgbHqCluster.payload_user_mtd
                })
                .from(summaryRgbHqCluster)
                .where(and(
                    eq(summaryRgbHqCluster.event_date, currDate),
                    eq(summaryRgbHqCluster.regional, 'PUMA'),
                    eq(summaryRgbHqCluster.catlos, 'all'),
                    eq(summaryRgbHqCluster.brand, 'all')
                ))
                .groupBy(summaryRgbHqCluster.cluster)
                .as('b')

            const clusterTargetRevenue = db
                .select({
                    cluster: territoryArea4.cluster,
                    target_paying_subs: sum(feiTargetPuma.paying_subs).as('target_paying_subs')
                })
                .from(feiTargetPuma)
                .rightJoin(territoryArea4, eq(feiTargetPuma.territory, territoryArea4.kabupaten))
                .where(eq(feiTargetPuma.periode, yyyyMM))
                .groupBy(territoryArea4.cluster)
                .as('c')

            const summaryRevCluster = db
                .select({
                    cluster: summaryRevAllCluster.cluster,
                    revAllM: summaryRevAllCluster.rev_all_m,
                    revAllM1: sql<number>`SUM(CASE WHEN ${summaryRevAllCluster.tgl} = ${prevDate} THEN ${summaryRevAllCluster.rev_all_m} END)`.as('rev_all_m1'),
                    revAllM12: sql<number>`SUM(CASE WHEN ${summaryRevAllCluster.tgl} = ${prevYearCurrDate} THEN ${summaryRevAllCluster.rev_all_m} END)`.as('rev_all_m12'),

                    revBbM: summaryRevAllCluster.rev_bb_m,
                    revBbM1: sql<number>`SUM(CASE WHEN ${summaryRevAllCluster.tgl} = ${prevDate} THEN ${summaryRevAllCluster.rev_bb_m} END)`.as('rev_bb_m1'),
                    revBbM12: sql<number>`SUM(CASE WHEN ${summaryRevAllCluster.tgl} = ${prevYearCurrDate} THEN ${summaryRevAllCluster.rev_bb_m} END)`.as('rev_bb_m12'),

                    revVoiceM: summaryRevAllCluster.rev_voice_m,
                    revVoiceM1: sql<number>`SUM(CASE WHEN ${summaryRevAllCluster.tgl} = ${prevDate} THEN ${summaryRevAllCluster.rev_voice_m} END)`.as('rev_voice_m1'),
                    revVoiceM12: sql<number>`SUM(CASE WHEN ${summaryRevAllCluster.tgl} = ${prevYearCurrDate} THEN ${summaryRevAllCluster.rev_voice_m} END)`.as('rev_voice_m12'),

                    revDigitalM: summaryRevAllCluster.rev_digital_m,
                    revDigitalM1: sql<number>`SUM(CASE WHEN ${summaryRevAllCluster.tgl} = ${prevDate} THEN ${summaryRevAllCluster.rev_digital_m} END)`.as('rev_digital_m1'),
                    revDigitalM12: sql<number>`SUM(CASE WHEN ${summaryRevAllCluster.tgl} = ${prevYearCurrDate} THEN ${summaryRevAllCluster.rev_digital_m} END)`.as('rev_digital_m12')
                })
                .from(summaryRevAllCluster)
                .where(and(
                    eq(summaryRevAllCluster.regional, 'PUMA'),
                    eq(summaryRevAllCluster.newAbcStrate, 'all')
                ))
                .groupBy(summaryRevAllCluster.cluster)
                .as('d')

            const revenueCluster = db
                .select({
                    name: clusterTerritory.cluster,
                    target_rgb_all: sql<number>`ROUND(SUM(${clusterTargetRevenue.target_paying_subs}),0)`.as('target_rgb_all'),
                    mtd_rgb: sql<number>`ROUND(SUM(${rgbCluster.mtdRgb}),0)`.as('mtd_rgb'),
                    mom_rgb: sql<string>`CONCAT(ROUND(((SUM(${rgbCluster.mtdRgb})/SUM(${rgbCluster.rgbLastMonth}))-1)*100,2),'%')`.as('mom_rgb'),
                    yoy_rgb: sql<string>`CONCAT(ROUND(((SUM(${rgbCluster.mtdRgb})/SUM(${rgbCluster.rgbLastYear}))-1)*100,2),'%')`.as('yoy_rgb'),

                    ach_target_fm_rgb: sql<string>`CONCAT(ROUND((SUM(${rgbCluster.mtdRgb})/SUM(${clusterTargetRevenue.target_paying_subs}))*100,2),'%')`.as('ach_target_fm_rgb'),
                    drr_rgb: sql<string>`CONCAT(ROUND((SUM(${rgbCluster.mtdRgb})/(${today}/${daysInMonth}*(SUM(${clusterTargetRevenue.target_paying_subs}))))*100,2),'%')`.as('drr_rgb'),
                    gap_to_target_rgb: sql<string>`ROUND((COALESCE(SUM(${rgbCluster.mtdRgb}) - SUM(${clusterTargetRevenue.target_paying_subs}),0)),0)`.as('gap_to_target_rgb'),
                    penetration_to_cb: sql<string>`CONCAT(ROUND((SUM(${rgbCluster.mtdRgb})/SUM(${rgbCluster.mtdCb})*100),2),'%')`.as('penetration_to_cb'),

                    mtd_rgb_data: sql<number>`ROUND(SUM(${rgbCluster.mtdRgbData}),0)`.as('mtd_rgb_data'),
                    mom_rgb_data: sql<string>`CONCAT(ROUND((((SUM(${rgbCluster.mtdRgbData})/SUM(${rgbCluster.rgbDataLastMonth}))-1)*100),2),'%')`.as('mom_rgb_data'),
                    yoy_rgb_data: sql<string>`CONCAT(ROUND((((SUM(${rgbCluster.mtdRgbData})/SUM(${rgbCluster.rgbDataLastYear}))-1)*100),2),'%')`.as('yoy_rgb_data'),

                    mtd_rgb_digital: sql<number>`ROUND(SUM(${rgbCluster.mtdRgbDigital}),0)`.as('mtd_rgb_digital'),
                    mom_rgb_digital: sql<string>`CONCAT(ROUND((((SUM(${rgbCluster.mtdRgbDigital})/SUM(${rgbCluster.rgbDigitalLastMonth}))-1)*100),2),'%')`.as('mom_rgb_digital'),
                    yoy_rgb_digital: sql<string>`CONCAT(ROUND((((SUM(${rgbCluster.mtdRgbDigital})/SUM(${rgbCluster.rgbDigitalLastYear}))-1)*100),2),'%')`.as('yoy_rgb_digital'),

                    mtd_arpu: sql<number>`ROUND(SUM(${summaryRevCluster.revAllM})/SUM(${rgbCluster.mtdRgb}),0)`.as('mtd_arpu'),
                    mom_arpu: sql<string>`CONCAT(ROUND((((SUM(${summaryRevCluster.revAllM})/SUM(${rgbCluster.mtdRgb}))/(SUM(${summaryRevCluster.revAllM1})/SUM(${rgbCluster.rgbDataLastMonth})))-1)*100,2),'%')`.as('mom_arpu'),
                    yoy_arpu: sql<string>`CONCAT(ROUND((((SUM(${summaryRevCluster.revAllM})/SUM(${rgbCluster.mtdRgb}))/(SUM(${summaryRevCluster.revAllM12})/SUM(${rgbCluster.rgbDataLastYear})))-1)*100,2),'%')`.as('yoy_arpu'),

                    mtd_arpu_data: sql<number>`ROUND(SUM(${summaryRevCluster.revBbM})/SUM(${rgbCluster.mtdRgbData}),0)`.as('mtd_arpu_data'),
                    mom_arpu_data: sql<string>`CONCAT(ROUND((((SUM(${summaryRevCluster.revBbM})/SUM(${rgbCluster.mtdRgbData}))/(SUM(${summaryRevCluster.revBbM1})/SUM(${rgbCluster.rgbDataLastMonth})))-1)*100,2),'%')`.as('mom_arpu_data'),
                    yoy_arpu_data: sql<string>`CONCAT(ROUND((((SUM(${summaryRevCluster.revBbM})/SUM(${rgbCluster.mtdRgbData}))/(SUM(${summaryRevCluster.revBbM12})/SUM(${rgbCluster.rgbDataLastYear})))-1)*100,2),'%')`.as('yoy_arpu_data'),

                    mtd_arpu_digital: sql<number>`ROUND(SUM(${summaryRevCluster.revDigitalM})/SUM(${rgbCluster.mtdRgbDigital}),0)`.as('mtd_arpu_digital'),
                    mom_arpu_digital: sql<string>`CONCAT(ROUND((((SUM(${summaryRevCluster.revDigitalM})/SUM(${rgbCluster.mtdRgbDigital}))/(SUM(${summaryRevCluster.revDigitalM1})/SUM(${rgbCluster.rgbDigitalLastMonth})))-1)*100,2),'%')`.as('mom_arpu_digital'),
                    yoy_arpu_digital: sql<string>`CONCAT(ROUND((((SUM(${summaryRevCluster.revDigitalM})/SUM(${rgbCluster.mtdRgbDigital}))/(SUM(${summaryRevCluster.revDigitalM12})/SUM(${rgbCluster.rgbDigitalLastYear})))-1)*100,2),'%')`.as('yoy_arpu_digital'),

                    mtd_rpmb: sql<number>`ROUND(SUM(${summaryRevCluster.revAllM})/(SUM(${rgbCluster.mtdPayload})/1024),2)`.as('mtd_rpmb'),
                    mom_rpmb: sql<string>`CONCAT(ROUND(((SUM(${summaryRevCluster.revAllM})/(SUM(${rgbCluster.mtdPayload})/1024)) / (SUM(${summaryRevCluster.revAllM1})/(SUM(${rgbCluster.payloadLastMonth})/1024))-1)*100,2),'%')`.as('mom_rpmb'),
                    yoy_rpmb: sql<string>`CONCAT(ROUND(((SUM(${summaryRevCluster.revAllM})/(SUM(${rgbCluster.mtdPayload})/1024)) / (SUM(${summaryRevCluster.revAllM12})/(SUM(${rgbCluster.payloadLastYear})/1024))-1)*100,2),'%')`.as('yoy_rpmb'),
                })
                .from(clusterTerritory)
                .leftJoin(rgbCluster, eq(clusterTerritory.cluster, rgbCluster.cluster))
                .leftJoin(clusterTargetRevenue, eq(clusterTerritory.cluster, clusterTargetRevenue.cluster))
                .leftJoin(summaryRevCluster, eq(clusterTerritory.cluster, summaryRevCluster.cluster))
                .groupBy(clusterTerritory.cluster)

            const kabupatenHeaderQuery = db
                .selectDistinct({
                    name: sql<string | null>`'KABUPATEN'`,
                    target_rgb_all: sql<number>`''`,
                    mtd_rgb: sql<number>`''`,
                    mom_rgb: sql<string>`''`,
                    yoy_rgb: sql<string>`''`,
                    ach_target_fm_rgb: sql<string>`''`,
                    drr_rgb: sql<string>`''`,
                    gap_to_target_rgb: sql<string>`''`,
                    penetration_to_cb: sql<string>`''`,

                    mtd_rgb_data: sql<number>`''`,
                    mom_rgb_data: sql<string>`''`,
                    yoy_rgb_data: sql<string>`''`,

                    mtd_rgb_digital: sql<number>`''`,
                    mom_rgb_digital: sql<string>`''`,
                    yoy_rgb_digital: sql<string>`''`,

                    mtd_arpu: sql<number>`''`,
                    mom_arpu: sql<string>`''`,
                    yoy_arpu: sql<string>`''`,

                    mtd_arpu_data: sql<number>`''`,
                    mom_arpu_data: sql<string>`''`,
                    yoy_arpu_data: sql<string>`''`,

                    mtd_arpu_digital: sql<number>`''`,
                    mom_arpu_digital: sql<string>`''`,
                    yoy_arpu_digital: sql<string>`''`,

                    mtd_rpmb: sql<number>`''`,
                    mom_rpmb: sql<string>`''`,
                    yoy_rpmb: sql<string>`''`,
                })
                .from(feiTargetPuma)

            const kabupatenTerritory = db
                .select({
                    kabupaten: territoryArea4.kabupaten,
                })
                .from(territoryArea4)
                .where(branch && subbranch && cluster && kabupaten ? and(
                    eq(territoryArea4.regional, 'PUMA'),
                    eq(territoryArea4.branch, branch),
                    eq(territoryArea4.subbranch, subbranch),
                    eq(territoryArea4.cluster, cluster),
                    eq(territoryArea4.kabupaten, kabupaten),
                ) : branch && subbranch && cluster ? and(
                    eq(territoryArea4.regional, 'PUMA'),
                    eq(territoryArea4.branch, branch),
                    eq(territoryArea4.subbranch, subbranch),
                    eq(territoryArea4.cluster, cluster),
                ) : branch && subbranch ? and(
                    eq(territoryArea4.regional, 'PUMA'),
                    eq(territoryArea4.branch, branch),
                    eq(territoryArea4.subbranch, subbranch),
                ) : branch ? and(
                    eq(territoryArea4.regional, 'PUMA'),
                    eq(territoryArea4.branch, branch),
                ) : eq(territoryArea4.regional, 'PUMA'))
                .groupBy(territoryArea4.kabupaten)
                .as('a')

            const rgbKabupaten = db
                .select({
                    kabupaten: summaryRgbHqKabupaten.kabupaten,
                    cbLastYear: summaryRgbHqKabupaten.cb_m12,
                    cbLastMonth: summaryRgbHqKabupaten.cb_m1,
                    mtdCb: summaryRgbHqKabupaten.cb_mtd,
                    rgbLastYear: summaryRgbHqKabupaten.rgb_m12,
                    rgbLastMonth: summaryRgbHqKabupaten.rgb_m1,
                    mtdRgb: summaryRgbHqKabupaten.rgb_mtd,
                    rgbDataLastYear: summaryRgbHqKabupaten.rgb_data_m12,
                    rgbDataLastMonth: summaryRgbHqKabupaten.rgb_data_m1,
                    mtdRgbData: summaryRgbHqKabupaten.rgb_data_mtd,
                    rgbDigitalLastYear: summaryRgbHqKabupaten.rgb_digital_m12,
                    rgbDigitalLastMonth: summaryRgbHqKabupaten.rgb_digital_m1,
                    mtdRgbDigital: summaryRgbHqKabupaten.rgb_digital_mtd,
                    payloadLastYear: summaryRgbHqKabupaten.payload_user_m12,
                    payloadLastMonth: summaryRgbHqKabupaten.payload_user_m1,
                    mtdPayload: summaryRgbHqKabupaten.payload_user_mtd
                })
                .from(summaryRgbHqKabupaten)
                .where(and(
                    eq(summaryRgbHqKabupaten.event_date, currDate),
                    eq(summaryRgbHqKabupaten.regional, 'PUMA'),
                    eq(summaryRgbHqKabupaten.catlos, 'all'),
                    eq(summaryRgbHqKabupaten.brand, 'all')
                ))
                .groupBy(summaryRgbHqKabupaten.kabupaten)
                .as('b')

            const kabupatenTargetRevenue = db
                .select({
                    kabupaten: territoryArea4.kabupaten,
                    target_paying_subs: sum(feiTargetPuma.paying_subs).as('target_paying_subs')
                })
                .from(feiTargetPuma)
                .rightJoin(territoryArea4, eq(feiTargetPuma.territory, territoryArea4.kabupaten))
                .where(eq(feiTargetPuma.periode, yyyyMM))
                .groupBy(territoryArea4.kabupaten)
                .as('c')

            const summaryRevKabupaten = db
                .select({
                    kabupaten: summaryRevAllKabupaten.kabupaten,
                    revAllM: summaryRevAllKabupaten.rev_all_m,
                    revAllM1: sql<number>`SUM(CASE WHEN ${summaryRevAllKabupaten.tgl} = ${prevDate} THEN ${summaryRevAllKabupaten.rev_all_m} END)`.as('rev_all_m1'),
                    revAllM12: sql<number>`SUM(CASE WHEN ${summaryRevAllKabupaten.tgl} = ${prevYearCurrDate} THEN ${summaryRevAllKabupaten.rev_all_m} END)`.as('rev_all_m12'),

                    revBbM: summaryRevAllKabupaten.rev_bb_m,
                    revBbM1: sql<number>`SUM(CASE WHEN ${summaryRevAllKabupaten.tgl} = ${prevDate} THEN ${summaryRevAllKabupaten.rev_bb_m} END)`.as('rev_bb_m1'),
                    revBbM12: sql<number>`SUM(CASE WHEN ${summaryRevAllKabupaten.tgl} = ${prevYearCurrDate} THEN ${summaryRevAllKabupaten.rev_bb_m} END)`.as('rev_bb_m12'),

                    revVoiceM: summaryRevAllKabupaten.rev_voice_m,
                    revVoiceM1: sql<number>`SUM(CASE WHEN ${summaryRevAllKabupaten.tgl} = ${prevDate} THEN ${summaryRevAllKabupaten.rev_voice_m} END)`.as('rev_voice_m1'),
                    revVoiceM12: sql<number>`SUM(CASE WHEN ${summaryRevAllKabupaten.tgl} = ${prevYearCurrDate} THEN ${summaryRevAllKabupaten.rev_voice_m} END)`.as('rev_voice_m12'),

                    revDigitalM: summaryRevAllKabupaten.rev_digital_m,
                    revDigitalM1: sql<number>`SUM(CASE WHEN ${summaryRevAllKabupaten.tgl} = ${prevDate} THEN ${summaryRevAllKabupaten.rev_digital_m} END)`.as('rev_digital_m1'),
                    revDigitalM12: sql<number>`SUM(CASE WHEN ${summaryRevAllKabupaten.tgl} = ${prevYearCurrDate} THEN ${summaryRevAllKabupaten.rev_digital_m} END)`.as('rev_digital_m12')
                })
                .from(summaryRevAllKabupaten)
                .where(and(
                    eq(summaryRevAllKabupaten.regional, 'PUMA'),
                    eq(summaryRevAllKabupaten.newAbcStrate, 'all')
                ))
                .groupBy(summaryRevAllKabupaten.kabupaten)
                .as('d')

            const revenueKabupaten = db
                .select({
                    name: kabupatenTerritory.kabupaten,
                    target_rgb_all: sql<number>`ROUND(SUM(${kabupatenTargetRevenue.target_paying_subs}),0)`.as('target_rgb_all'),
                    mtd_rgb: sql<number>`ROUND(SUM(${rgbKabupaten.mtdRgb}),0)`.as('mtd_rgb'),
                    mom_rgb: sql<string>`CONCAT(ROUND(((SUM(${rgbKabupaten.mtdRgb})/SUM(${rgbKabupaten.rgbLastMonth}))-1)*100,2),'%')`.as('mom_rgb'),
                    yoy_rgb: sql<string>`CONCAT(ROUND(((SUM(${rgbKabupaten.mtdRgb})/SUM(${rgbKabupaten.rgbLastYear}))-1)*100,2),'%')`.as('yoy_rgb'),

                    ach_target_fm_rgb: sql<string>`CONCAT(ROUND((SUM(${rgbKabupaten.mtdRgb})/SUM(${kabupatenTargetRevenue.target_paying_subs}))*100,2),'%')`.as('ach_target_fm_rgb'),
                    drr_rgb: sql<string>`CONCAT(ROUND((SUM(${rgbKabupaten.mtdRgb})/(${today}/${daysInMonth}*(SUM(${kabupatenTargetRevenue.target_paying_subs}))))*100,2),'%')`.as('drr_rgb'),
                    gap_to_target_rgb: sql<string>`ROUND((COALESCE(SUM(${rgbKabupaten.mtdRgb}) - SUM(${kabupatenTargetRevenue.target_paying_subs}),0)),0)`.as('gap_to_target_rgb'),
                    penetration_to_cb: sql<string>`CONCAT(ROUND((SUM(${rgbKabupaten.mtdRgb})/SUM(${rgbKabupaten.mtdCb})*100),2),'%')`.as('penetration_to_cb'),

                    mtd_rgb_data: sql<number>`ROUND(SUM(${rgbKabupaten.mtdRgbData}),0)`.as('mtd_rgb_data'),
                    mom_rgb_data: sql<string>`CONCAT(ROUND((((SUM(${rgbKabupaten.mtdRgbData})/SUM(${rgbKabupaten.rgbDataLastMonth}))-1)*100),2),'%')`.as('mom_rgb_data'),
                    yoy_rgb_data: sql<string>`CONCAT(ROUND((((SUM(${rgbKabupaten.mtdRgbData})/SUM(${rgbKabupaten.rgbDataLastYear}))-1)*100),2),'%')`.as('yoy_rgb_data'),

                    mtd_rgb_digital: sql<number>`ROUND(SUM(${rgbKabupaten.mtdRgbDigital}),0)`.as('mtd_rgb_digital'),
                    mom_rgb_digital: sql<string>`CONCAT(ROUND((((SUM(${rgbKabupaten.mtdRgbDigital})/SUM(${rgbKabupaten.rgbDigitalLastMonth}))-1)*100),2),'%')`.as('mom_rgb_digital'),
                    yoy_rgb_digital: sql<string>`CONCAT(ROUND((((SUM(${rgbKabupaten.mtdRgbDigital})/SUM(${rgbKabupaten.rgbDigitalLastYear}))-1)*100),2),'%')`.as('yoy_rgb_digital'),

                    mtd_arpu: sql<number>`ROUND(SUM(${summaryRevKabupaten.revAllM})/SUM(${rgbKabupaten.mtdRgb}),0)`.as('mtd_arpu'),
                    mom_arpu: sql<string>`CONCAT(ROUND((((SUM(${summaryRevKabupaten.revAllM})/SUM(${rgbKabupaten.mtdRgb}))/(SUM(${summaryRevKabupaten.revAllM1})/SUM(${rgbKabupaten.rgbDataLastMonth})))-1)*100,2),'%')`.as('mom_arpu'),
                    yoy_arpu: sql<string>`CONCAT(ROUND((((SUM(${summaryRevKabupaten.revAllM})/SUM(${rgbKabupaten.mtdRgb}))/(SUM(${summaryRevKabupaten.revAllM12})/SUM(${rgbKabupaten.rgbDataLastYear})))-1)*100,2),'%')`.as('yoy_arpu'),

                    mtd_arpu_data: sql<number>`ROUND(SUM(${summaryRevKabupaten.revBbM})/SUM(${rgbKabupaten.mtdRgbData}),0)`.as('mtd_arpu_data'),
                    mom_arpu_data: sql<string>`CONCAT(ROUND((((SUM(${summaryRevKabupaten.revBbM})/SUM(${rgbKabupaten.mtdRgbData}))/(SUM(${summaryRevKabupaten.revBbM1})/SUM(${rgbKabupaten.rgbDataLastMonth})))-1)*100,2),'%')`.as('mom_arpu_data'),
                    yoy_arpu_data: sql<string>`CONCAT(ROUND((((SUM(${summaryRevKabupaten.revBbM})/SUM(${rgbKabupaten.mtdRgbData}))/(SUM(${summaryRevKabupaten.revBbM12})/SUM(${rgbKabupaten.rgbDataLastYear})))-1)*100,2),'%')`.as('yoy_arpu_data'),

                    mtd_arpu_digital: sql<number>`ROUND(SUM(${summaryRevKabupaten.revDigitalM})/SUM(${rgbKabupaten.mtdRgbDigital}),0)`.as('mtd_arpu_digital'),
                    mom_arpu_digital: sql<string>`CONCAT(ROUND((((SUM(${summaryRevKabupaten.revDigitalM})/SUM(${rgbKabupaten.mtdRgbDigital}))/(SUM(${summaryRevKabupaten.revDigitalM1})/SUM(${rgbKabupaten.rgbDigitalLastMonth})))-1)*100,2),'%')`.as('mom_arpu_digital'),
                    yoy_arpu_digital: sql<string>`CONCAT(ROUND((((SUM(${summaryRevKabupaten.revDigitalM})/SUM(${rgbKabupaten.mtdRgbDigital}))/(SUM(${summaryRevKabupaten.revDigitalM12})/SUM(${rgbKabupaten.rgbDigitalLastYear})))-1)*100,2),'%')`.as('yoy_arpu_digital'),

                    mtd_rpmb: sql<number>`ROUND(SUM(${summaryRevKabupaten.revAllM})/(SUM(${rgbKabupaten.mtdPayload})/1024),2)`.as('mtd_rpmb'),
                    mom_rpmb: sql<string>`CONCAT(ROUND(((SUM(${summaryRevKabupaten.revAllM})/(SUM(${rgbKabupaten.mtdPayload})/1024)) / (SUM(${summaryRevKabupaten.revAllM1})/(SUM(${rgbKabupaten.payloadLastMonth})/1024))-1)*100,2),'%')`.as('mom_rpmb'),
                    yoy_rpmb: sql<string>`CONCAT(ROUND(((SUM(${summaryRevKabupaten.revAllM})/(SUM(${rgbKabupaten.mtdPayload})/1024)) / (SUM(${summaryRevKabupaten.revAllM12})/(SUM(${rgbKabupaten.payloadLastYear})/1024))-1)*100,2),'%')`.as('yoy_rpmb'),
                })
                .from(kabupatenTerritory)
                .leftJoin(rgbKabupaten, eq(kabupatenTerritory.kabupaten, rgbKabupaten.kabupaten))
                .leftJoin(kabupatenTargetRevenue, eq(kabupatenTerritory.kabupaten, kabupatenTargetRevenue.kabupaten))
                .leftJoin(summaryRevKabupaten, eq(kabupatenTerritory.kabupaten, summaryRevKabupaten.kabupaten))
                .groupBy(kabupatenTerritory.kabupaten)

            const [finalDataRevenue] = await Promise.all([
                unionAll(revenueRegional, branchHeaderQuery, revenueBranch, subbranchHeaderQuery, revenueSubbranch, clusterHeaderQuery, revenueCluster, kabupatenHeaderQuery, revenueKabupaten)
            ])

            return c.json(finalDataRevenue, 200)
        })

export default app