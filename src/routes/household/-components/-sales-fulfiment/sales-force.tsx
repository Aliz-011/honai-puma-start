import { endOfMonth, format, startOfMonth } from "date-fns"

import {
    Card,
    CardContent,
    CardDescription,
    CardHeader,
    CardTitle,
} from "@/components/ui/card"

type SalesForceData = {
    name: string | null;
    sf_black: number;
    sf_bronze: number;
    sf_silver: number;
    sf_gold: number;
    sf_platinum: number;
    total_sf: number;
}

export const SalesForce = ({ data, selectedDate }: { data: SalesForceData[]; selectedDate: Date }) => {
    const lastDayOfSelectedMonth = endOfMonth(selectedDate);
    const isEndOfMonth = selectedDate.getDate() === lastDayOfSelectedMonth.getDate();

    const endOfCurrMonth = isEndOfMonth ? lastDayOfSelectedMonth : selectedDate;

    const currStartOfMonth = format(startOfMonth(selectedDate), 'd')
    const currDate = format(endOfCurrMonth, 'dd MMM yyyy')

    return (
        <Card className="@container/card flex flex-col max-h-[480px] shadow-none border border-gray-200 bg-white dark:border-gray-800 dark:bg-white/3 px-0.5 ">
            <CardHeader>
                <CardTitle>Sales Force</CardTitle>
                <CardDescription>{currStartOfMonth} - {currDate}</CardDescription>
            </CardHeader>
            <CardContent className="space-y-4 max-h-full">

                <div className="flex items-center">
                    <div className="w-32 text-base font-medium">Black</div>
                    <div className="flex-1 mx-4 relative">
                        <div className="bg-gray-800/20 h-8 rounded-lg flex items-center justify-center relative overflow-hidden">
                            <div
                                className="absolute left-0 top-0 h-full bg-gray-800 transition-all duration-1000 ease-out flex items-center justify-center"
                                style={{ width: `${(data[0].sf_black / data[0].total_sf * 100).toFixed(2)}%` }}
                            >
                                <span className="text-white text-sm font-medium px-2">{data[0].sf_black}</span>
                            </div>
                            <span className="absolute right-3 text-gray-800 text-sm font-medium">
                                {(data[0].sf_black / data[0].total_sf * 100).toFixed(2)}%
                            </span>
                        </div>
                    </div>
                </div>

                <div className="flex items-center justify-between">
                    <div className="w-32 text-base font-medium">Bronze</div>
                    <div className="flex-1 mx-4 relative">
                        <div className="bg-amber-800/20 h-8 rounded-lg flex items-center justify-center relative overflow-hidden">
                            <div
                                className="absolute left-0 top-0 h-full bg-amber-800 transition-all duration-1000 ease-out flex items-center justify-center"
                                style={{ width: `${(data[0].sf_bronze / data[0].total_sf * 100).toFixed(2)}%` }}
                            >
                                <span className="text-white text-sm font-medium px-2">{data[0].sf_bronze}</span>
                            </div>
                            <span className="absolute right-3 text-amber-800 text-sm font-medium">
                                {(data[0].sf_bronze / data[0].total_sf * 100).toFixed(2)}%
                            </span>
                        </div>
                    </div>
                </div>

                <div className="flex items-center justify-between">

                    <div className="w-32 text-base font-medium">Silver</div>
                    <div className="flex-1 mx-4 relative">
                        <div className="bg-gray-400/20 h-8 rounded-lg flex items-center justify-center relative overflow-hidden">
                            <div
                                className="absolute left-0 top-0 h-full bg-gray-400 transition-all duration-1000 ease-out flex items-center justify-center"
                                style={{ width: `${(data[0].sf_silver / data[0].total_sf * 100).toFixed(2)}%` }}
                            >
                                <span className="text-white text-sm font-medium px-2">{data[0].sf_silver}</span>
                            </div>
                            <span className="absolute right-3 text-gray-800 text-sm font-medium">
                                {(data[0].sf_silver / data[0].total_sf * 100).toFixed(2)}%
                            </span>
                        </div>
                    </div>
                </div>

                <div className="flex items-center justify-between">
                    <div className="w-32 text-base font-medium">Gold</div>
                    <div className="flex-1 mx-4 relative">
                        <div className="bg-yellow-800/20 h-8 rounded-lg flex items-center justify-center relative overflow-hidden">
                            <div
                                className="absolute left-0 top-0 h-full bg-yellow-600 transition-all duration-1000 ease-out flex items-center justify-center"
                                style={{ width: `${(data[0].sf_gold / data[0].total_sf * 100).toFixed(2)}%` }}
                            >
                                <span className="text-white text-sm font-medium px-2">{data[0].sf_gold}</span>
                            </div>
                            <span className="absolute right-3 text-yellow-800 text-sm font-medium">
                                {(data[0].sf_gold / data[0].total_sf * 100).toFixed(2)}%
                            </span>
                        </div>
                    </div>
                </div>

                <div className="flex items-center justify-between">
                    <div className="w-32 text-base font-medium">Platinum</div>
                    <div className="flex-1 mx-4 relative">
                        <div className="bg-gray-100 h-8 rounded-lg flex items-center justify-center relative overflow-hidden">
                            <div
                                className="absolute left-0 top-0 h-full bg-slate-300 transition-all duration-1000 ease-out flex items-center justify-center"
                                style={{ width: `${(data[0].sf_platinum / data[0].total_sf * 100).toFixed(2)}%` }}
                            >
                                <span className="text-gray-800 text-sm font-medium px-2">{data[0].sf_platinum}</span>
                            </div>
                            <span className="absolute right-3 text-gray-500 text-sm font-medium">
                                {(data[0].sf_platinum / data[0].total_sf * 100).toFixed(2)}%
                            </span>
                        </div>
                    </div>
                </div>

                <div className="mt-6 pt-4 border-t border-gray-200">
                    <div className="flex items-center justify-between">
                        <span className="text-base font-semibold text-gray-800">Total</span>
                        <span className="text-lg font-bold text-gray-800">{data[0].total_sf}</span>
                    </div>
                </div>

            </CardContent>
        </Card >
    )
}