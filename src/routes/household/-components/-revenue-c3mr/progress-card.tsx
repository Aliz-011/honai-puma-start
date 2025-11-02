import { cn } from "@/lib/utils";

type ProgressCardParams = {
    data: {
        label: string;
        value: number | string;
        style?: string;
    }[];
    title: string;
    subtitle: string;
}

export const ProgressCard = ({ data, title, subtitle }: ProgressCardParams) => {
    return (
        <div className="bg-white/95 backdrop-blur-lg rounded-lg p-8 border border-white/5 relative overflow-hidden">
            <div className="absolute top-0 left-0 right-0 h-1 bg-linear-to-r from-amber-500 to-orange-600"></div>

            <div className="flex items-center gap-3 mb-6">
                <div className="w-8 h-8 bg-linear-to-br from-amber-500 to-orange-600 rounded-lg flex items-center justify-center text-white font-bold text-sm">
                    {subtitle}
                </div>
                <h3 className="text-lg font-semibold text-gray-800">{title}</h3>
            </div>

            <div className="space-y-4">
                {data.map((metric, index) => (
                    <div key={index} className="flex justify-between items-center border-b border-gray-100 last:border-b-0">
                        <span className="text-gray-600 font-medium text-sm">{metric.label}</span>
                        <span className={cn("px-2 py-1.5 rounded-lg font-bold text-sm", metric.style)}>
                            {metric.value}
                        </span>
                    </div>
                ))}
            </div>
        </div>
    );
};