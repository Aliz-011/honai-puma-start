import * as React from "react"
import {
    PieChart,
    Settings2,
    Command,
    Target,
    HandCoins,
    HouseWifi,
} from "lucide-react"

import { NavMain } from "./nav-main"
import { NavSecondary } from "./nav-secondary"
import {
    Sidebar,
    SidebarContent,
    SidebarFooter,
    SidebarHeader,
    SidebarMenu,
    SidebarMenuButton,
    SidebarMenuItem,
    SidebarRail,
} from "@/components/ui/sidebar"
import { Link } from "@tanstack/react-router"
import { NavUser } from "@/components/nav-user"

const data = {
    user: {
        name: "shadcn",
        email: "m@example.com",
        avatar: "/avatars/shadcn.jpg",
    },
    navMain: [
        {
            title: "Revenue Gross",
            url: "#",
            icon: PieChart,
            items: [
                {
                    title: "Revenue Gross All",
                    url: "/puma/revenue-gross",
                },
                {
                    title: "Revenue ByU",
                    url: "/puma/revenue-byu",
                },
                {
                    title: "Revenue Gross Prabayar",
                    url: "/puma/revenue-gross-prabayar",
                },
            ],
        },
        {
            title: "Target SO",
            url: "#",
            icon: Target,
            items: [
                {
                    title: "SO All",
                    url: "/puma/target-so",
                }
            ],
        },
        {
            title: "Revenue New Sales",
            url: "#",
            icon: PieChart,
            items: [
                {
                    title: "Revenue New Sales All",
                    url: "/puma/revenue-new-sales/",
                },
                {
                    title: "Revenue New Sales Prabayar",
                    url: "/puma/revenue-new-sales-prabayar/",
                },
            ],
        },
        {
            title: "Paying Subs",
            url: "#",
            icon: HandCoins,
            items: [
                {
                    title: "Paying Subs All",
                    url: "/puma/paying-subs",
                },
                {
                    title: "Paying LoS 0-1 All",
                    url: "/puma/paying-los",
                }
            ],
        },
        {
            title: "Revenue CVM",
            url: "#",
            icon: PieChart,
            items: [
                {
                    title: "Revenue CVM All",
                    url: "/puma/revenue-cvm/",
                },
                {
                    title: "Revenue CVM Outlet",
                    url: "/puma/revenue-cvm-outlet",
                }
            ],
        },
        {
            title: "Revenue Redeem PV",
            url: "#",
            icon: PieChart,
            items: [
                {
                    title: "Revenue Redeem PV Prabyar",
                    url: "/puma/revenue-redeem-pv",
                }
            ],
        },
        {
            title: "Revenue SA",
            url: "#",
            icon: PieChart,
            items: [
                {
                    title: "Revenue SA All",
                    url: "/puma/revenue-sa",
                },
            ],
        },
        {
            title: "Trx New Sales",
            url: "#",
            icon: Target,
            items: [
                {
                    title: "Trx New Sales All",
                    url: "/puma/trx-new-sales",
                },
                {
                    title: "Trx New Sales Prabayar",
                    url: "/puma/trx-new-sales-prabayar",
                },
                {
                    title: "Trx New Sales ByU",
                    url: "/puma/trx-new-sales-byu",
                },
            ],
        },
    ],
    navSecondary: [
        {
            title: "Settings",
            url: "/puma/account/appearance",
            icon: Settings2,
        },
    ]
}

export function AppSidebar({ ...props }: React.ComponentProps<typeof Sidebar>) {
    return (
        <Sidebar collapsible="offcanvas" {...props}>
            <SidebarHeader>
                <SidebarMenu>
                    <SidebarMenuItem>
                        <SidebarMenuButton size="lg" asChild>
                            <Link to="/">
                                <div className="flex aspect-square size-8 items-center justify-center rounded-lg bg-sidebar-primary text-sidebar-primary-foreground">
                                    <Command className="size-4" />
                                </div>
                                <div className="grid flex-1 text-left text-sm leading-tight">
                                    <span className="truncate font-semibold">PUMA</span>
                                    <span className="truncate text-xs">Enterprise</span>
                                </div>
                            </Link>
                        </SidebarMenuButton>
                    </SidebarMenuItem>
                </SidebarMenu>
            </SidebarHeader>
            <SidebarContent className="sidebar">
                <NavMain items={data.navMain} />
                <NavSecondary items={data.navSecondary} className="mt-auto" />
            </SidebarContent>
            <SidebarFooter>
                <NavUser />
            </SidebarFooter>
            <SidebarRail />
        </Sidebar>
    )
}
