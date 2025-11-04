import { clsx, type ClassValue } from 'clsx'
import { twMerge } from 'tailwind-merge'

export function cn(...inputs: ClassValue[]) {
  return twMerge(clsx(inputs))
}


export function formatToBillion(val: number) {
  if (val == undefined || val == null || !val) {
    return null
  }

  return Number(val).toLocaleString('id-ID', {
    maximumFractionDigits: 2,
    minimumFractionDigits: 0,
  })
}

export function getGrowthColor(value: string) {
  const numeric = value ? parseFloat(value.replace('%', '')) : 0
  return numeric > 0;
};

export function getAchGrowthColor(value: string) {
  const numeric = value ? parseFloat(value.replace('%', '')) : 0
  return numeric >= 100;
};