import { type RemixiconComponentType } from '@remixicon/react'
import { useLocation } from '@tanstack/react-router'

import {
    SidebarGroup,
    SidebarGroupLabel,
    SidebarMenu,
    SidebarMenuButton,
    SidebarMenuItem
} from "@/components/ui/sidebar"
import { Link } from '@tanstack/react-router';
import { cn } from '@/lib/utils';

type NavItem = {
    title: string;
    url: string;
    icon?: RemixiconComponentType;
    isActive?: boolean;
    items?: {
        title: string;
        url: string;
    }[]
}

export const NavMain = ({ items }: { items: NavItem[] }) => {
    const location = useLocation()
    return (
        <SidebarGroup>
            <SidebarGroupLabel>Platform</SidebarGroupLabel>
            <SidebarMenu>
                {items.map((item) => {
                    const isActive = location.pathname === item.url
                    return (
                        <SidebarMenuItem key={item.title}>
                            <SidebarMenuButton tooltip={item.title} className={cn('font-medium', isActive ? 'bg-orange-200 hover:bg-orange-200/80' : '')} asChild>
                                <Link to={item.url}>
                                    {item.icon && <item.icon />}
                                    <span>{item.title}</span>
                                </Link>
                            </SidebarMenuButton>
                        </SidebarMenuItem>
                    )
                })}
            </SidebarMenu>
        </SidebarGroup>
    )
}