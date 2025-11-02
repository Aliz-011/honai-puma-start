import { db } from "@/db";
import { dynamicLacciSiteId } from "@/db/schema/hadoop";
import { all_territory_puma_2024, territoryHousehold } from "@/db/schema/puma_2025";
import { createServerFn } from "@tanstack/react-start";
import { format, subDays } from "date-fns";

export const getBranches = createServerFn({ method: 'GET' })
    .handler(async () => await db.select({ territory: all_territory_puma_2024.branch }).from(all_territory_puma_2024).groupBy(all_territory_puma_2024.branch))

export const getSubbranches = createServerFn({ method: 'GET' })
    .handler(async () => await db.select({ territory: all_territory_puma_2024.subbranch, branch: all_territory_puma_2024.branch }).from(all_territory_puma_2024).groupBy(all_territory_puma_2024.subbranch))

export const getClusters = createServerFn({ method: 'GET' })
    .handler(async () => await db.select({ territory: all_territory_puma_2024.cluster, subbranch: all_territory_puma_2024.subbranch, branch: all_territory_puma_2024.branch }).from(all_territory_puma_2024).groupBy(all_territory_puma_2024.cluster))

export const getKabupatens = createServerFn({ method: 'GET' })
    .handler(async () => await db.select({ territory: all_territory_puma_2024.kabupaten, cluster: all_territory_puma_2024.cluster, branch: all_territory_puma_2024.branch }).from(all_territory_puma_2024).groupBy(all_territory_puma_2024.kabupaten))

export const getKecamatans = createServerFn({ method: 'GET' })
    .handler(async () => await db.select().from(all_territory_puma_2024).groupBy(all_territory_puma_2024.kecamatan))

export const getWoks = createServerFn({ method: 'GET' })
    .handler(async () => await db.select({ wok: territoryHousehold.wok, branch: territoryHousehold.branch }).from(territoryHousehold).groupBy(territoryHousehold.wok))

export const getStos = createServerFn({ method: 'GET' })
    .handler(async () => await db.select({ sto: territoryHousehold.sto, wok: territoryHousehold.wok }).from(territoryHousehold).groupBy(territoryHousehold.sto))

export const getSiteIds = createServerFn({ method: 'GET' })
    .handler(async () => {
        const currentTime = new Date()
        const period = format(subDays(currentTime, 2), 'yyyyMM')

        const lacciSiteid = dynamicLacciSiteId(period)

        const siteIds = await db
            .selectDistinct({ site_id: lacciSiteid.site_id })
            .from(lacciSiteid)

        return siteIds
    })