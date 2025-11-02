import type { QueryObserverResult, RefetchOptions } from "@tanstack/react-query"
import { endOfMonth, intlFormat, subDays } from "date-fns"

import { Button } from "@/components/ui/button"
import {
    Table,
    TableBody,
    TableCaption,
    TableCell,
    TableHead,
    TableHeader,
    TableRow,
} from "@/components/ui/table"
import { Card, CardHeader, CardTitle, CardContent } from '@/components/ui/card'
import { Skeleton } from "@/components/ui/skeleton"

import { cn, formatToBillion, getAchGrowthColor, getGrowthColor } from "@/lib/utils"
import { Download } from "lucide-react"

type Params = {
    data: Revenue[] | undefined;
    date?: string;
    latestUpdatedData: number;
    isLoading?: boolean;
    title: string;
    refetch: (options?: RefetchOptions) => Promise<QueryObserverResult<Revenue[], Error>>
}

export function DataTable({ latestUpdatedData: daysBehind, refetch, title, data, isLoading, date }: Params) {
    const selectedDate = date ? new Date(date) : subDays(new Date(), daysBehind) // today - 2 days
    const lastDayOfSelectedMonth = endOfMonth(selectedDate);
    const isEndOfMonth = selectedDate.getDate() === lastDayOfSelectedMonth.getDate();

    const endOfCurrMonth = isEndOfMonth ? lastDayOfSelectedMonth : selectedDate;

    if (isLoading) {
        return (
            <div className="w-[1104px] overflow-x-auto remove-scrollbar">
                <div className="w-full">
                    <div className="flex flex-col space-y-3">
                        <Skeleton className="h-4 w-[200px]" />
                        <Skeleton className="h-[275px] w-[1104px] rounded-xl" />
                    </div>
                </div>
            </div>
        )
    }

    if (!data || !Array.isArray(data)) {
        console.error('Invalid data shape:', data); // For debugging
        return (
            <div className="w-[1104px] overflow-x-auto remove-scrollbar">
                <div className="w-full p-4 text-red-500">
                    No data available. <button onClick={() => refetch()}>Retry</button>
                </div>
            </div>
        );
    }

    const handleDownload = async () => {
        // exportToExcel(formattedData, format(selectedDate, 'd MMM'), `revenue_gross_${format(selectedDate, 'yyyyMM')}`)
    }

    return (
        <Card>
            <CardHeader className="flex items-center justify-between">
                <CardTitle>{title}</CardTitle>
                <Button variant='outline' size='sm' className="cursor-pointer" onClick={handleDownload}>
                    <Download className="size-4" />
                </Button>
            </CardHeader>
            <CardContent>
                <Table>
                    <TableCaption>A list of territories and their revenues</TableCaption>
                    <TableHeader>
                        <TableRow>
                            <TableHead rowSpan={2} className="font-medium border-r dark:border-gray-700 text-white text-center bg-rose-700">Territory</TableHead>
                            <TableHead colSpan={12} className="font-medium border bg-blue-500 text-gray-50 text-center dark:text-white dark:border-gray-800">{title}</TableHead>
                        </TableRow>
                        <TableRow>
                            <TableHead className="whitespace-nowrap font-medium text-white bg-zinc-950 border-r last:border-r-0 dark:border-r-gray-700 text-center">Target</TableHead>
                            <TableHead className="whitespace-nowrap font-medium text-white bg-zinc-950 border-r last:border-r-0 dark:border-r-gray-700 text-center">{intlFormat(endOfCurrMonth, { month: 'short', day: 'numeric' }, { locale: "id-ID" })}</TableHead>
                            <TableHead className="whitespace-nowrap font-medium text-white bg-zinc-950 border-r last:border-r-0 dark:border-r-gray-700 text-center">
                                Gap
                            </TableHead>
                            <TableHead className="whitespace-nowrap font-medium text-white bg-zinc-950 border-r last:border-r-0 dark:border-r-gray-700 text-center">
                                <div className="flex items-center justify-center">
                                    Ach FM
                                </div>
                            </TableHead>
                            <TableHead className="whitespace-nowrap font-medium text-white bg-zinc-950 border-r last:border-r-0 dark:border-r-gray-700 text-center">
                                <div className="flex items-center justify-center">
                                    Ach DRR
                                </div>
                            </TableHead>
                            <TableHead className="whitespace-nowrap font-medium text-white bg-zinc-950 border-r last:border-r-0 dark:border-r-gray-700 text-center">
                                <div className="flex items-center justify-center">
                                    MoM(%)
                                </div>
                            </TableHead>
                            <TableHead className="whitespace-nowrap font-medium text-white bg-zinc-950 border-r last:border-r-0 dark:border-r-gray-700 text-center">
                                Abs
                            </TableHead>
                            <TableHead className="whitespace-nowrap font-medium text-white bg-zinc-950 border-r last:border-r-0 dark:border-r-gray-700 text-center">
                                <div className="flex items-center justify-center">
                                    YoY
                                </div>
                            </TableHead>
                            <TableHead className="whitespace-nowrap font-medium text-white bg-zinc-950 border-r last:border-r-0 dark:border-r-gray-700 text-center">
                                <div className="flex items-center justify-center">
                                    YtD
                                </div>
                            </TableHead>
                        </TableRow>
                    </TableHeader>
                    <TableBody>
                        <TableRow>
                            <TableCell colSpan={14} className="px-1 py-0.5 border-r last:border-r-0 text-start font-semibold bg-gray-200 dark:text-white dark:border-gray-800 dark:bg-white/3">
                                REGION
                            </TableCell>
                        </TableRow>

                        {data?.map((item, index) => {
                            const isHeaderRow = ['REGIONAL', 'BRANCH', 'SUBBRANCH', 'CLUSTER', 'KABUPATEN'].includes(item.name?.toUpperCase()!);

                            return (
                                <TableRow key={`${item.name}-${index}`}>
                                    <TableCell
                                        colSpan={isHeaderRow ? 14 : 1}
                                        className={cn(
                                            "px-1 py-0.5 border-r last:border-r-0 text-start",
                                            isHeaderRow ? 'font-semibold bg-gray-200 dark:text-white dark:border-gray-800 dark:bg-white/3' : 'font-normal dark:text-white dark:border-gray-800'
                                        )}
                                    >
                                        {item.name}
                                    </TableCell>

                                    {!isHeaderRow && (
                                        <>
                                            <TableCell className="px-1 py-0.5 border-r last:border-r-0 text-end dark:text-white dark:border-gray-800 tabular-nums">
                                                <span className='text-end'>{formatToBillion(item.targetAll)}</span>
                                            </TableCell>
                                            <TableCell className="px-1 py-0.5 border-r last:border-r-0 text-end dark:text-white dark:border-gray-800 tabular-nums">
                                                <span className='text-end'>{formatToBillion(item.revAll)}</span>
                                            </TableCell>
                                            <TableCell className={cn("px-1 py-0.5 border-r last:border-r-0 text-end dark:text-white dark:border-gray-800 tabular-nums", item.gapToTargetAll > 0 ? 'text-green-500' : 'text-rose-500')}>
                                                <span>{formatToBillion(item.gapToTargetAll)}</span>
                                            </TableCell>
                                            <TableCell className={cn("px-1 py-0.5 border-r last:border-r-0 text-end text-theme-xs dark:text-white dark:border-gray-800 tabular-nums", getAchGrowthColor(item.achTargetFmAll) ? 'text-green-500' : 'text-rose-500')}>
                                                {item.achTargetFmAll}
                                            </TableCell>
                                            <TableCell className={cn("px-1 py-0.5 border-r last:border-r-0 text-end text-theme-xs dark:text-white dark:border-gray-800 tabular-nums", getAchGrowthColor(item.drrAll) ? 'text-green-500' : 'text-rose-500')}>
                                                {item.drrAll}
                                            </TableCell>
                                            <TableCell className={cn("px-1 py-0.5 border-r last:border-r-0 text-end text-theme-xs font-medium dark:border-gray-800 tabular-nums", getGrowthColor(item.momAll) ? 'text-green-500' : 'text-rose-500')}>
                                                {item.momAll}
                                            </TableCell>
                                            <TableCell className={cn("px-1 py-0.5 border-r last:border-r-0 text-end dark:text-white dark:border-gray-800 tabular-nums", item.revAllAbsolut > 0 ? 'text-green-500' : 'text-rose-500')}>
                                                <span className='text-end'>{formatToBillion(item.revAllAbsolut)}</span>
                                            </TableCell>
                                            <TableCell className={cn("px-1 py-0.5 border-r last:border-r-0 text-end text-theme-xs dark:text-white dark:border-gray-800 tabular-nums", getGrowthColor(item.yoyAll) ? 'text-green-500' : 'text-rose-500')}>
                                                {item.yoyAll}
                                            </TableCell>
                                            <TableCell className={cn("px-1 py-0.5 border-r last:border-r-0 text-end text-theme-xs dark:text-white dark:border-gray-800 tabular-nums", getGrowthColor(item.ytdAll) ? 'text-green-500' : 'text-rose-500')}>
                                                {item.ytdAll}
                                            </TableCell>
                                        </>
                                    )}
                                </TableRow>
                            );
                        })}

                    </TableBody>
                </Table>
            </CardContent>
        </Card>
    )
}

type Revenue = {
    name: string | null;
    targetAll: number;
    revAll: number;
    achTargetFmAll: string;
    drrAll: string;
    gapToTargetAll: number;
    momAll: string;
    revAllAbsolut: number;
    yoyAll: string;
    ytdAll: string;
}