import { endOfMonth, format, subMonths } from "date-fns"

import {
    Card,
    CardContent,
    CardDescription,
    CardFooter,
    CardHeader,
    CardTitle,
} from "@/components/ui/card"

import type { SalesFulfilmentResponseData } from '@/types'
import { Separator } from "@/components/ui/separator"

export const FunnelingGroup = ({ data, selectedDate }: { data: SalesFulfilmentResponseData[]; selectedDate: Date }) => {

    const lastDayOfSelectedMonth = endOfMonth(selectedDate);
    const isEndOfMonth = selectedDate.getDate() === lastDayOfSelectedMonth.getDate();

    const endOfCurrMonth = isEndOfMonth ? lastDayOfSelectedMonth : selectedDate;
    const endOfPrevMonth = isEndOfMonth ? endOfMonth(subMonths(selectedDate, 1)) : subMonths(selectedDate, 1);

    const currDate = format(endOfCurrMonth, 'dd MMM yyyy')
    const prevMonth = format(endOfPrevMonth, 'MMM')

    return (
        <Card className="rounded-xl border border-gray-200 bg-white dark:border-gray-800 dark:bg-white/3">
            <CardHeader>
                <CardTitle className="text-base font-semibold">Funneling Group</CardTitle>
                <CardDescription>{prevMonth} - {currDate}</CardDescription>
            </CardHeader>

            <CardContent className="flex items-center justify-center">
                <div className="w-full max-w-5xl">
                    {/* Funnel Steps */}
                    <div className="space-y-3">
                        {/* Registration */}
                        <div className="flex items-center">
                            <div className="w-28 text-sm font-normal">Registration</div>
                            <div className="flex-1 mx-4 relative">
                                <div className="bg-chart-1/20 h-8 rounded-lg flex items-center justify-center relative overflow-hidden">
                                    <div
                                        className="absolute left-0 top-0 h-full bg-chart-1 transition-all duration-1000 ease-out flex items-center justify-center"
                                        style={{ width: `${Math.max(parseFloat(data[0].registration_per.replace('%', '')), 8)}%` }}
                                    >
                                        <span className="text-white text-sm font-medium px-2">{data[0].registration}</span>
                                    </div>
                                    <span className="absolute right-3 text-chart-1 text-sm font-medium">
                                        {data[0].registration_per}
                                    </span>
                                </div>
                            </div>
                        </div>

                        {/* Provision Issued */}
                        <div className="flex items-center">
                            <div className="w-28 text-sm font-normal">Provision Issued</div>
                            <div className="flex-1 mx-4 relative">
                                <div className="bg-chart-2/20 h-8 rounded-lg flex items-center justify-center relative overflow-hidden">
                                    <div
                                        className="absolute left-0 top-0 h-full bg-chart-2 transition-all duration-1000 ease-out flex items-center justify-center"
                                        style={{ width: `${Math.max(parseFloat(data[0].provision_issued_per.replace('%', '')), 8)}%` }}
                                    >
                                        <span className="text-white text-sm font-medium px-2">{data[0].provision_issued}</span>
                                    </div>
                                    <span className="absolute right-3 text-chart-2 text-sm font-medium">
                                        {data[0].provision_issued_per}
                                    </span>
                                </div>
                            </div>
                        </div>

                        {/* Provision Completed */}
                        <div className="flex items-center">
                            <div className="w-28 text-sm font-normal">Provision Completed</div>
                            <div className="flex-1 mx-4 relative">
                                <div className="bg-chart-3/20 h-8 rounded-lg flex items-center justify-center relative overflow-hidden">
                                    <div
                                        className="absolute left-0 top-0 h-full bg-chart-3 transition-all duration-1000 ease-out flex items-center justify-center"
                                        style={{ width: `${Math.max(parseFloat(data[0].provision_completed_per.replace('%', '')), 8)}%` }}
                                    >
                                        <span className="text-white text-sm font-medium px-2">{data[0].provision_completed}</span>
                                    </div>
                                    <span className="absolute right-3 text-chart-3 text-sm font-medium">
                                        {data[0].provision_completed_per}
                                    </span>
                                </div>
                            </div>
                        </div>

                        {/* Activation Completed */}
                        <div className="flex items-center">
                            <div className="w-28 text-sm font-normal">Activation Completed</div>
                            <div className="flex-1 mx-4 relative">
                                <div className="bg-chart-4/20 h-8 rounded-lg flex items-center justify-center relative overflow-hidden">
                                    <div
                                        className="absolute left-0 top-0 h-full bg-chart-4 transition-all duration-1000 ease-out flex items-center justify-center"
                                        style={{ width: `${Math.max(parseFloat(data[0].activation_completed_per.replace('%', '')), 8)}%` }}
                                    >
                                        <span className="text-white text-sm font-medium px-2">{data[0].activation_completed}</span>
                                    </div>
                                    <span className="absolute right-3 text-chart-4 text-sm font-medium">
                                        {data[0].activation_completed_per}
                                    </span>
                                </div>
                            </div>
                        </div>

                        {/* Cancelled */}
                        <div className="flex items-center">
                            <div className="w-28 text-sm font-normal">Cancelled</div>
                            <div className="flex-1 mx-4 relative">
                                <div className="bg-chart-5/20 h-8 rounded-lg flex items-center justify-center relative overflow-hidden">
                                    <div
                                        className="absolute left-0 top-0 h-full bg-chart-5 transition-all duration-1000 ease-out flex items-center justify-center"
                                        style={{ width: `${Math.max(parseFloat(data[0].cancelled_per.replace('%', '')), 8)}%` }}
                                    >
                                        <span className="text-white text-sm font-medium px-2">{data[0].cancelled}</span>
                                    </div>
                                    <span className="absolute right-3 text-chart-5 text-sm font-medium">
                                        {data[0].cancelled_per}
                                    </span>
                                </div>
                            </div>
                        </div>

                        {/* Fallout */}
                        <div className="flex items-center">
                            <div className="w-28 text-sm font-normal">Fallout</div>
                            <div className="flex-1 mx-4 relative">
                                <div className="bg-rose-700/20 h-8 rounded-lg flex items-center justify-center relative overflow-hidden">
                                    <div
                                        className="absolute left-0 top-0 h-full bg-rose-700/80 transition-all duration-1000 ease-out flex items-center justify-center"
                                        style={{ width: `${parseFloat(data[0].fallout_per.replace('%', ''))}%` }}
                                    >
                                        <span className="text-white text-sm font-medium px-2 mr-auto">{data[0].fallout}</span>
                                    </div>
                                    <span className="absolute right-3 text-rose-800/80 text-sm font-medium">
                                        {data[0].fallout_per}
                                    </span>
                                </div>
                            </div>
                        </div>
                    </div>
                </div>
            </CardContent>
            <Separator />
            <CardFooter className="flex items-center justify-between gap-2 text-sm">
                <div className="flex gap-2 leading-none font-medium">
                    Total
                </div>
                <div className="font-semibold text-lg">
                    {data[0].total_re_non_ps}
                </div>
            </CardFooter>
        </Card >


    )
}

export const WOLoS = ({ data, selectedDate }: { data: SalesFulfilmentResponseData[]; selectedDate: Date }) => {
    const lastDayOfSelectedMonth = endOfMonth(selectedDate);
    const isEndOfMonth = selectedDate.getDate() === lastDayOfSelectedMonth.getDate();

    const endOfCurrMonth = isEndOfMonth ? lastDayOfSelectedMonth : selectedDate;
    const endOfPrevMonth = isEndOfMonth ? endOfMonth(subMonths(selectedDate, 1)) : subMonths(selectedDate, 1);

    const currDate = format(endOfCurrMonth, 'dd MMM yyyy')
    const prevMonth = format(endOfPrevMonth, 'MMM')

    return (
        <Card className="w-full rounded-xl border border-gray-200 bg-white dark:border-gray-800 dark:bg-white/[0.03]">
            <CardHeader>
                <CardTitle className="text-base font-semibold">WO need to follow up (By LoS WO)</CardTitle>
                <CardDescription>{prevMonth} - {currDate}</CardDescription>
            </CardHeader>

            <CardContent className="flex items-center justify-center">
                <div className="w-full max-w-5xl">
                    {/* Funnel Steps */}
                    <div className="space-y-3">
                        {/* Registration */}
                        <div className="flex items-center">
                            <div className="w-28 text-sm font-normal">3 Days</div>
                            <div className="flex-1 mx-4 relative">
                                <div className="bg-chart-1/20 h-8 rounded-lg flex items-center justify-center relative overflow-hidden">
                                    <div
                                        className="absolute left-0 top-0 h-full bg-chart-1 transition-all duration-1000 ease-out flex items-center justify-center"
                                        style={{ width: `${Math.max(parseFloat(data[0].wo_3_per.replace('%', '')), 8)}%` }}
                                    >
                                        <span className="text-white text-sm font-medium px-2">{data[0].wo_3}</span>
                                    </div>
                                    <span className="absolute right-3 text-chart-1 text-sm font-medium">
                                        {data[0].wo_3_per}
                                    </span>
                                </div>
                            </div>
                        </div>

                        {/* Provision Issued */}
                        <div className="flex items-center">
                            <div className="w-28 text-sm font-normal">4 - 7 Days</div>
                            <div className="flex-1 mx-4 relative">
                                <div className="bg-chart-2/20 h-8 rounded-lg flex items-center justify-center relative overflow-hidden">
                                    <div
                                        className="absolute left-0 top-0 h-full bg-chart-2 transition-all duration-1000 ease-out flex items-center justify-center"
                                        style={{ width: `${Math.max(parseFloat(data[0].wo_4_7_per.replace('%', '')), 8)}%` }}
                                    >
                                        <span className="text-white text-sm font-medium px-2">{data[0].wo_4_7}</span>
                                    </div>
                                    <span className="absolute right-3 text-chart-2 text-sm font-medium">
                                        {data[0].wo_4_7_per}
                                    </span>
                                </div>
                            </div>
                        </div>

                        {/* Provision Completed */}
                        <div className="flex items-center">
                            <div className="w-28 text-sm font-normal">8 - 14 Days</div>
                            <div className="flex-1 mx-4 relative">
                                <div className="bg-chart-3/20 h-8 rounded-lg flex items-center justify-center relative overflow-hidden">
                                    <div
                                        className="absolute left-0 top-0 h-full bg-chart-3 transition-all duration-1000 ease-out flex items-center justify-center"
                                        style={{ width: `${Math.max(parseFloat(data[0].wo_8_14_per.replace('%', '')), 8)}%` }}
                                    >
                                        <span className="text-white text-sm font-medium px-2">{data[0].wo_8_14}</span>
                                    </div>
                                    <span className="absolute right-3 text-chart-3 text-sm font-medium">
                                        {data[0].wo_8_14_per}
                                    </span>
                                </div>
                            </div>
                        </div>

                        {/* Activation Completed */}
                        <div className="flex items-center">
                            <div className="w-28 text-sm font-normal">15 - 30 Days</div>
                            <div className="flex-1 mx-4 relative">
                                <div className="bg-chart-4/20 h-8 rounded-lg flex items-center justify-center relative overflow-hidden">
                                    <div
                                        className="absolute left-0 top-0 h-full bg-chart-4 transition-all duration-1000 ease-out flex items-center justify-center"
                                        style={{ width: `${Math.max(parseFloat(data[0].wo_15_30_per.replace('%', '')), 8)}%` }}
                                    >
                                        <span className="text-white text-sm font-medium px-2">{data[0].wo_15_30}</span>
                                    </div>
                                    <span className="absolute right-3 text-chart-4 text-sm font-medium">
                                        {data[0].wo_15_30_per}
                                    </span>
                                </div>
                            </div>
                        </div>

                        {/* Cancelled */}
                        <div className="flex items-center">
                            <div className="w-28 text-sm font-normal">{'>'}30 Days</div>
                            <div className="flex-1 mx-4 relative">
                                <div className="bg-chart-5/20 h-8 rounded-lg flex items-center justify-center relative overflow-hidden">
                                    <div
                                        className="absolute left-0 top-0 h-full bg-chart-5 transition-all duration-1000 ease-out flex items-center justify-center"
                                        style={{ width: `${Math.max(parseFloat(data[0].wo_gt_30_per.replace('%', '')), 8)}%` }}
                                    >
                                        <span className="text-white text-sm font-medium px-2">{data[0].wo_gt_30}</span>
                                    </div>
                                    <span className="absolute right-3 text-chart-5 text-sm font-medium">
                                        {data[0].wo_gt_30_per}
                                    </span>
                                </div>
                            </div>
                        </div>
                    </div>
                </div>
            </CardContent>
            <Separator />
            <CardFooter className="flex items-center justify-between gap-2 text-sm">
                <div className="flex gap-2 leading-none font-medium">
                    Total
                </div>
                <div className="font-semibold text-lg">
                    {data[0].total_wo}
                </div>
            </CardFooter>
        </Card >


    )
}