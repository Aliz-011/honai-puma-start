import { endOfMonth, format, startOfMonth } from "date-fns"
import { Bar, Line, ComposedChart, CartesianGrid, XAxis, YAxis, Pie, PieChart, LabelList } from "recharts"

import {
    Card,
    CardContent,
    CardDescription,
    CardHeader,
    CardTitle,
} from "@/components/ui/card"
import {
    ChartConfig,
    ChartContainer,
    ChartTooltip,
    ChartTooltipContent,
    ChartLegend,
    ChartLegendContent,
} from "@/components/ui/chart"
import { type SalesFulfilmentResponseData } from "@/types"
import { Separator } from "@/components/ui/separator"
import { cn } from "@/lib/utils"

const chartConfig = {
    target: {
        label: "Target",
        color: "hsl(0, 84%, 60%)",
    },
    actual: {
        label: "Actual",
        color: "hsl(120, 61%, 50%)",
    },
    drr: {
        label: "Daily Run Rate",
        color: "hsl(217, 91%, 60%)",
    },
} satisfies ChartConfig


type IOREPSParams = {
    date: string;
    title: string;
    data: {
        territory: string | null;
        target: number;
        actual: number;
        drr: number;
        color?: string;
        date?: string;
        ach_fm: string;
    }[];
}

export function IOREPSChart({ date, data: chartData, title }: IOREPSParams) {
    return (
        <Card className="@container/card shadow-none border border-gray-200 bg-white dark:border-gray-800 dark:bg-white/[0.03]">
            <CardHeader className="relative">
                <CardTitle>{title}</CardTitle>
                <CardDescription>{date}</CardDescription>
            </CardHeader>
            <CardContent className="pt-0 pb-2.5 px-0">
                <ChartContainer config={chartConfig} className="aspect-auto h-[280px] w-full">
                    <ComposedChart data={chartData} margin={{ left: 0, right: 0, bottom: 0, top: 20 }}>
                        <CartesianGrid vertical={false} strokeDasharray="3 3" />
                        <XAxis
                            dataKey="territory"
                            tickLine={false}
                            axisLine={false}
                            textAnchor="end"
                            interval={0}
                        />
                        <YAxis
                            yAxisId="bars"
                            orientation="left"
                            tickLine={false}
                            axisLine={false}
                        />
                        <YAxis
                            yAxisId="line"
                            orientation="right"
                            tickLine={false}
                            axisLine={false}
                        />
                        <ChartTooltip
                            content={<ChartTooltipContent />}
                        />
                        <ChartLegend content={<ChartLegendContent />} />
                        <Bar
                            yAxisId="bars"
                            dataKey="target"
                            fill="var(--color-target)"
                            radius={[2, 2, 0, 0]}
                            maxBarSize={40}
                        >
                            <LabelList
                                position="top"
                                offset={12}
                                className="fill-foreground"
                                fontSize={12}
                            />
                        </Bar>
                        <Bar
                            yAxisId="bars"
                            dataKey="actual"
                            fill="var(--color-actual)"
                            radius={[2, 2, 0, 0]}
                            maxBarSize={40}
                        >
                            <LabelList
                                position="top"
                                offset={12}
                                className="fill-foreground"
                                fontSize={12}
                            />
                        </Bar>
                        <Line
                            yAxisId="line"
                            type="monotone"
                            dataKey="drr"
                            stroke="var(--color-drr)"
                            strokeWidth={3}
                            dot={{ fill: "var(--color-drr)", strokeWidth: 2, r: 4 }}
                        >
                            <LabelList
                                position="top"
                                offset={12}
                                className="fill-foreground"
                                fontSize={12}
                                formatter={(value: number) => `${value}%`}
                            />
                        </Line>
                    </ComposedChart>
                </ChartContainer>
            </CardContent>
        </Card>
    )
}

export function BrownGreenChart({ data: chartData, date, title }: IOREPSParams) {
    return (
        <Card className="@container/card shadow-none border border-gray-200 bg-white dark:border-gray-800 dark:bg-white/[0.03]">
            <CardHeader className="relative">
                <CardTitle>{title}</CardTitle>
                <CardDescription>{date}</CardDescription>
            </CardHeader>
            <CardContent className="pt-0 pb-2.5 px-0">
                <ChartContainer config={chartConfig} className="aspect-auto h-[280px] w-full">
                    <ComposedChart data={chartData} margin={{ left: 0, right: 0, bottom: 0, top: 20 }}>
                        <CartesianGrid vertical={false} strokeDasharray="3 3" />
                        <XAxis
                            dataKey="territory"
                            tickLine={false}
                            axisLine={false}
                            textAnchor="end"
                            interval={0}
                        />
                        <YAxis
                            yAxisId="bars"
                            orientation="left"
                            tickLine={false}
                            axisLine={false}
                        />
                        <YAxis
                            yAxisId="line"
                            orientation="right"
                            tickLine={false}
                            axisLine={false}
                        />
                        <ChartTooltip
                            content={<ChartTooltipContent className="w-[220px]" />}
                        />
                        <ChartLegend content={<ChartLegendContent />} />
                        <Bar
                            yAxisId="bars"
                            dataKey="target"
                            fill="var(--color-target)"
                            radius={[2, 2, 0, 0]}
                            maxBarSize={40}
                        >
                            <LabelList
                                position="top"
                                offset={12}
                                className="fill-foreground"
                                fontSize={12}
                            />
                        </Bar>
                        <Bar
                            yAxisId="bars"
                            dataKey="actual"
                            fill="var(--color-actual)"
                            radius={[2, 2, 0, 0]}
                            maxBarSize={40}
                        >
                            <LabelList
                                position="top"
                                offset={12}
                                className="fill-foreground"
                                fontSize={12}
                            />
                        </Bar>
                        <Line
                            yAxisId="line"
                            type="monotone"
                            dataKey="drr"
                            stroke="var(--color-drr)"
                            strokeWidth={3}
                            dot={{ fill: "var(--color-drr)", strokeWidth: 2, r: 4 }}
                        >
                            <LabelList
                                position="top"
                                offset={12}
                                className="fill-foreground"
                                fontSize={12}
                                formatter={(value: number) => `${value}%`}
                            />
                        </Line>
                    </ComposedChart>
                </ChartContainer>
            </CardContent>
        </Card>
    )
}

const chartConfigPie = {
    visitors: {
        label: "Channel",
    },
    digital: {
        label: "Digital",
        color: "var(--color-sky-400)",
    },
    grapari: {
        label: "GraPari",
        color: "var(--color-sky-500)",
    },
    agency: {
        label: "Agency",
        color: "var(--color-red-800)",
    },
    community: {
        label: "Community",
        color: "var(--color-sky-600)",
    }
} satisfies ChartConfig

type PieParams = {
    date: string;
    data: SalesFulfilmentResponseData[]
}

export function ChartPie({ data, date }: PieParams) {
    const chartData = [
        { channel: "digital", ach_fm: parseFloat(data[0].ach_fm_indihome.replace('%', '')), ach_fm_per: data[0].ach_fm_indihome, fill: "var(--color-digital)" },
        { channel: "grapari", ach_fm: parseFloat(data[0].ach_fm_grapari.replace('%', '')), ach_fm_per: data[0].ach_fm_grapari, fill: "var(--color-grapari)" },
        { channel: "agency", ach_fm: parseFloat(data[0].ach_fm_agency.replace('%', '')), ach_fm_per: data[0].ach_fm_agency, fill: "var(--color-agency)" },
        { channel: "community", ach_fm: parseFloat(data[0].ach_fm_community.replace('%', '')), ach_fm_per: data[0].ach_fm_community, fill: "var(--color-community)" },
    ]

    return (
        <Card className="@container/card flex flex-col max-h-[400px] shadow-none border border-gray-200 bg-white dark:border-gray-800 dark:bg-white/[0.03] px-0.5">
            <CardHeader>
                <CardTitle>PS By Channel</CardTitle>
                <CardDescription>{date}</CardDescription>
            </CardHeader>
            <CardContent className="max-h-full">
                <ChartContainer config={chartConfigPie} className="mx-auto aspect-square max-h-[300px] pb-0 pt-0">
                    <PieChart margin={{ top: 0, bottom: 10, left: 10, right: 10 }}>
                        <ChartTooltip content={<ChartTooltipContent labelKey="visitors" className='w-[180px]' />} />
                        <Pie
                            data={chartData}
                            dataKey="ach_fm"
                            nameKey="channel"
                            cx="50%"
                            cy="50%"
                            innerRadius={0}
                            outerRadius={80}
                            label={({ payload, percent }) => payload.ach_fm_per}
                            labelLine
                        />
                        <ChartLegend
                            content={<ChartLegendContent nameKey="channel" />}
                            className="my-4 flex-wrap gap-2 *:basis-1/4 *:justify-center"
                        />
                    </PieChart>
                </ChartContainer>
            </CardContent>
        </Card>
    )
}

export const ProgressCard = ({ date, data, title }: IOREPSParams) => {
    const selectedDate = new Date(data[0].date ?? date)
    const lastDayOfSelectedMonth = endOfMonth(selectedDate);
    const isEndOfMonth = selectedDate.getDate() === lastDayOfSelectedMonth.getDate();

    const endOfCurrMonth = isEndOfMonth ? lastDayOfSelectedMonth : selectedDate;

    const currStartOfMonth = format(startOfMonth(selectedDate), 'd')
    const currDate = format(endOfCurrMonth, 'dd MMM yyyy')

    return (
        <Card className="w-full text-center bg-white dark:bg-white/[0.03]">
            <CardHeader className="text-start">
                <div className="flex items-center justify-between">
                    <div>
                        <CardTitle className="text-lg font-semibold">{title}</CardTitle>
                        <CardDescription>{currStartOfMonth} - {currDate}</CardDescription>
                    </div>
                    <div className={cn("px-2 rounded-2xl", data[0].color)}>
                        <span className="text-sm text-white">DRR</span>
                    </div>
                </div>
            </CardHeader>
            <CardContent className="flex flex-col items-center justify-center p-6">
                {/* Circular Progress Bar */}
                <div className="relative w-48 h-4w-48">
                    <svg className="w-full h-full" viewBox="0 0 100 100">
                        {/* Background circle */}
                        <circle
                            className="text-gray-200"
                            strokeWidth="10"
                            stroke="currentColor"
                            fill="transparent"
                            r="40"
                            cx="50"
                            cy="50"
                        />
                        {/* Progress circle */}
                        <circle
                            className={data[0].color}
                            strokeWidth="10"
                            strokeDasharray={2 * Math.PI * 40} // Circumference of circle
                            strokeDashoffset={2 * Math.PI * 40 * (1 - data[0].drr / 100)}
                            strokeLinecap="round"
                            stroke="currentColor"
                            fill="transparent"
                            r="40"
                            cx="50"
                            cy="50"
                            style={{
                                transformOrigin: 'center',
                                transform: 'rotate(-90deg)',
                                transition: 'stroke-dashoffset 0.5s ease-in-out',
                            }}
                        />
                    </svg>
                    <div className="absolute inset-0 flex items-center justify-center text-3xl font-bold text-gray-800">
                        {data[0].drr}%
                    </div>
                </div>

                {/* Weekly and Monthly Progress */}
                <div className="flex justify-around w-full mt-8">
                    <div className="flex flex-col items-center">
                        <span className="text-lg font-bold text-gray-800">{data[0].target}</span>
                        <span className="text-base text-gray-500">Target</span>
                    </div>

                    <Separator orientation="vertical" />

                    <div className="flex flex-col items-center">
                        <span className="text-lg font-bold text-gray-800">{data[0].ach_fm}</span>
                        <span className="text-base text-gray-500">Ach</span>
                    </div>

                    <Separator orientation="vertical" />

                    <div className="flex flex-col items-center">
                        <span className="text-lg font-bold text-gray-800">{data[0].actual}</span>
                        <span className="text-base text-gray-500">Actual</span>
                    </div>
                </div>
            </CardContent>
        </Card>
    );
};