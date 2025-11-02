import * as React from "react"
import {
    Settings2,
    Command,
    HouseWifi,
    BarChartIcon,
    ListIcon,
    LayoutDashboardIcon,
} from "lucide-react"

import { NavMain } from "./nav-main"
import { NavUser } from "../../../components/nav-user"
import {
    Sidebar,
    SidebarContent,
    SidebarFooter,
    SidebarHeader,
    SidebarMenu,
    SidebarMenuButton,
    SidebarMenuItem,
} from "@/components/ui/sidebar"
import { Link } from "@tanstack/react-router"

const data = {
    user: {
        name: "shadcn",
        email: "m@example.com",
        avatar: "/avatars/shadcn.jpg",
    },
    navMain: [
        {
            title: "SALES FULFILMENT",
            url: "/household/sales-fulfilment",
            icon: LayoutDashboardIcon,
        },
        {
            title: "DEMANDS & DEPLOYMENT",
            url: "/household/demands-deployment",
            icon: ListIcon,
        },
        {
            title: "REVENUE & C3MR",
            url: "/household/revenue-c3mr",
            icon: BarChartIcon,
        }
    ],
    navSecondary: [
        {
            title: "Settings",
            url: "/puma/account/appearance",
            icon: Settings2,
        },
    ],
    projects: [
        {
            name: "FMC",
            url: "#",
            icon: HouseWifi,
            items: [
                {
                    title: "Line In Service",
                    url: "/puma/fmc/line-in-service",
                },
                {
                    title: "WL Connect Wifi",
                    url: "/puma/fmc/connect-wifi",
                }
            ],
        }
    ],
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
            </SidebarContent>
            <SidebarFooter>
                <NavUser />
            </SidebarFooter>
        </Sidebar>
    )
}
