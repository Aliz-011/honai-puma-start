import { Bar, Line, ComposedChart, CartesianGrid, XAxis, YAxis } from "recharts"

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

const chartConfig = {
    amount_port: {
        label: "Amount Port",
        color: "hsl(0, 84%, 60%)",
    },
    avail_port: {
        label: "Avail. Port",
        color: "hsl(120, 61%, 50%)",
    },
    occ_alpro: {
        label: "OCC Alpro",
        color: "var(--chart-4)"
    },
    mom_occ: {
        label: "MoM OCC",
        color: "hsl(217, 91%, 60%)",
    },
} satisfies ChartConfig

export type ChartDataItem = {
    name: string | null;
    target_all_sales: number;
    level: string;
    ach_target_fm_io_all_sales: number;
    drr_io: number;
    io_m: number;
    io_mom: string;
    io_gap_daily: number;
    ach_target_fm_re_all_sales: number;
    drr_re: number;
    re_m: number;
    re_mom: string;
    re_gap_daily: number;
    ach_target_fm_ps_all_sales: number;
    drr_ps: number;
    target_daily_ps: number;
    ach_daily_ps: number;
    ps_gap_daily: number;
    daily_ps_remaining: number;
    ps_m: number;
    ps_mom: string;
    ps_to_io: number;
    ps_to_re: number;
}

type Params = {
    date: string;
    data: ChartDataItem[]
}

const chartData2 = [
    { territory: 'AMBON', amount_port: 100, avail_port: 30, occ_alpro: 20, mom_occ: 50 }
]

export function IOChart({ date, data }: Params) {

    const chartData = data.map(item => ({
        territory: item.name,
        target: item.target_all_sales,
        actual: item.io_m,
        drr: item.drr_io
    }))

    return (
        <Card className="@container/card shadow-none">
            <CardHeader className="relative">
                <CardTitle>IO</CardTitle>
                <CardDescription>{date}</CardDescription>
            </CardHeader>
            <CardContent className="pt-0 pb-2.5 px-0">
                <ChartContainer config={chartConfig} className="aspect-auto h-[250px] w-full">
                    <ComposedChart data={chartData2} margin={{ left: 0, right: 0, bottom: 0 }}>
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
                            dataKey="amount_port"
                            fill="var(--color-amount_port)"
                            radius={[2, 2, 0, 0]}
                            maxBarSize={40}
                        />
                        <Bar
                            yAxisId="bars"
                            dataKey="avail_port"
                            fill="var(--color-avail_port)"
                            radius={[2, 2, 0, 0]}
                            maxBarSize={40}
                        />
                        <Bar
                            yAxisId="bars"
                            dataKey="occ_alpro"
                            fill="var(--color-occ_alpro)"
                            radius={[2, 2, 0, 0]}
                            maxBarSize={40}
                        />
                        <Line
                            yAxisId="line"
                            type="monotone"
                            dataKey="mom_occ"
                            stroke="var(--color-mom_occ)"
                            strokeWidth={3}
                            dot={{ fill: "var(--color-mom_occ)", strokeWidth: 2, r: 4 }}
                        />
                    </ComposedChart>
                </ChartContainer>
            </CardContent>
        </Card>
    )
}

export function REChart({ date, data }: Params) {

    const chartData = data.map(item => ({
        territory: item.name,
        target: item.target_all_sales,
        actual: item.re_m,
        drr: item.drr_re
    }))

    return (
        <Card className="@container/card shadow-none">
            <CardHeader className="relative">
                <CardTitle>RE</CardTitle>
                <CardDescription>{date}</CardDescription>
            </CardHeader>
            <CardContent className="pt-0 pb-2.5 px-0">
                <ChartContainer config={chartConfig} className="aspect-auto h-[250px] w-full">
                    <ComposedChart data={chartData2} margin={{ left: 0, right: 0, bottom: 10 }}>
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
                            dataKey="amount_port"
                            fill="var(--color-amount_port)"
                            radius={[2, 2, 0, 0]}
                            maxBarSize={40}
                        />
                        <Bar
                            yAxisId="bars"
                            dataKey="avail_port"
                            fill="var(--color-avail_port)"
                            radius={[2, 2, 0, 0]}
                            maxBarSize={40}
                        />
                        <Bar
                            yAxisId="bars"
                            dataKey="occ_alpro"
                            fill="var(--color-occ_alpro)"
                            radius={[2, 2, 0, 0]}
                            maxBarSize={40}
                        />

                        <Line
                            yAxisId="line"
                            type="monotone"
                            dataKey="mom_occ"
                            stroke="var(--color-mom_occ)"
                            strokeWidth={3}
                            dot={{ fill: "var(--color-mom_occ)", strokeWidth: 2, r: 4 }}
                        />
                    </ComposedChart>
                </ChartContainer>
            </CardContent>
            {/* <CardFooter className="flex-col items-start gap-2 text-sm">
                <div className="flex gap-2 font-medium leading-none">
                    Trending up by 5.2% this month <TrendingUp className="h-4 w-4" />
                </div>
            </CardFooter> */}
        </Card>
    )
}

export function PSChart({ date, data }: Params) {

    const chartData = data.map(item => ({
        territory: item.name,
        target: item.target_all_sales,
        actual: item.ps_m,
        drr: item.drr_ps
    }))

    return (
        <Card className="@container/card shadow-none">
            <CardHeader className="relative">
                <CardTitle>PS</CardTitle>
                <CardDescription>{date}</CardDescription>
            </CardHeader>
            <CardContent className="pt-0 pb-2.5 px-0">
                <ChartContainer config={chartConfig} className="aspect-auto h-[250px] w-full">
                    <ComposedChart data={chartData2} margin={{ left: 0, right: 0, bottom: 0 }}>
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
                            dataKey="amount_port"
                            fill="var(--color-amount_port)"
                            radius={[2, 2, 0, 0]}
                            maxBarSize={40}
                        />
                        <Bar
                            yAxisId="bars"
                            dataKey="avail_port"
                            fill="var(--color-avail_port)"
                            radius={[2, 2, 0, 0]}
                            maxBarSize={40}
                        />
                        <Bar
                            yAxisId="bars"
                            dataKey="occ_alpro"
                            fill="var(--color-occ_alpro)"
                            radius={[2, 2, 0, 0]}
                            maxBarSize={40}
                        />
                        <Line
                            yAxisId="line"
                            type="monotone"
                            dataKey="mom_occ"
                            stroke="var(--color-mom_occ)"
                            strokeWidth={3}
                            dot={{ fill: "var(--color-mom_occ)", strokeWidth: 2, r: 4 }}
                        />
                    </ComposedChart>
                </ChartContainer>
            </CardContent>            {/* <CardFooter className="flex-col items-start gap-2 text-sm">
                <div className="flex gap-2 font-medium leading-none">
                    Trending up by 5.2% this month <TrendingUp className="h-4 w-4" />
                </div>
            </CardFooter> */}
        </Card>
    )
}