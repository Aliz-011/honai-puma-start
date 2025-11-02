export interface RevenueBase {
    name: string;
    currMonthRevenue: number;
    currMonthTarget: number;
    currYtdRevenue: number;
    prevYtdRevenue: number;
    prevMonthRevenue: number;
    prevYearCurrMonthRevenue: number;
}

export interface Kabupaten extends RevenueBase { }

export interface Cluster extends RevenueBase {
    kabupatens: Kabupaten[]
}

export interface Subbranch extends RevenueBase {
    clusters: Cluster[];
};

export interface Branch extends RevenueBase {
    subbranches: Subbranch[];
};

export interface Regional extends RevenueBase {
    branches: Branch[];
};

export interface RevenueBaseFMC {
    name: string;
    revMtd: number;
    revM1: number;
    revM2: number;
    revM3: number;
    drMtd: number;
    drM1: number;
    drM2: number;
    drM3: number;
    subs: number;
    subsM1: number;
    subsM2: number;
    subsM3: number;
    payload: number;
    payloadM1: number;
    rgbAll: number;
    rgbVoice: number;
    rgbDigital: number;
    rgbData: number;
    rgbAllM1: number;
    rgbVoiceM1: number;
    rgbDigitalM1: number;
    rgbDataM1: number;
    rgbAllM2: number;
    rgbVoiceM2: number;
    rgbDigitalM2: number;
    rgbDataM2: number;
    rgbAllM3: number;
    rgbVoiceM3: number;
    rgbDigitalM3: number;
    rgbDataM3: number;
    arpu: number;
    arpuM1: number;
    arpuM2: number;
    arpuM3: number;
}

// STO (Store) level entity
export interface StoEntity extends RevenueBaseFMC {
    // No additional nested structures at the STO level
}

// WOK level entity
export interface WokEntity extends RevenueBaseFMC {
    stos: StoEntity[];
}

// Branch level entity
export interface BranchEntity extends RevenueBaseFMC {
    woks: WokEntity[];
}

// Region level entity (top level)
export interface RegionEntity extends RevenueBaseFMC {
    branches: BranchEntity[];
}

export interface CurrYtDRevenue {
    region: string;
    branch: string;
    subbranch: string;
    cluster: string;
    kabupaten: string;
    currYtdKabupatenRev: number;
    currYtdClusterRev: number
    currYtdSubbranchRev: number
    currYtdBranchRev: number
    currYtdRegionalRev: number
}
export interface PrevYtDRevenue {
    region: string;
    branch: string;
    subbranch: string;
    cluster: string;
    kabupaten: string;
    prevYtdKabupatenRev: number;
    prevYtdClusterRev: number
    prevYtdSubbranchRev: number
    prevYtdBranchRev: number
    prevYtdRegionalRev: number
}

export type SalesFulfilmentResponseData = {
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
    ach_fm_indihome: string;
    ps_indihome: number;
    ach_fm_grapari: string;
    ps_grapari: number;
    ach_fm_community: string;
    ps_community: number;
    ach_fm_agency: string;
    ps_sales_force: number;
    brownfield: number;
    target_brownfield: number;
    ach_fm_brownfield: string;
    drr_brownfield: string;
    greenfield: number;
    target_greenfield: number;
    ach_fm_greenfield: string;
    drr_greenfield: string;
    registration: number;
    provision_issued: number;
    provision_completed: number;
    activation_completed: number;
    fallout: number;
    cancelled: number;
    registration_per: string;
    provision_issued_per: string;
    provision_completed_per: string;
    activation_completed_per: string;
    fallout_per: string;
    cancelled_per: string;
    total_re_non_ps: number;
    kendala_pelanggan: number;
    kendala_sistem: number;
    kendala_teknik: number;
    kendala_others: number;
    total_all_kendala: number;

    INDIKASI_CABUT_PASANG: number;
    PENDING: number;
    PELANGGAN_MASIH_RAGU: number;
    RNA: number;
    KENDALA_IZIN: number;
    BATAL: number;
    RUMAH_KOSONG: number;
    DOUBLE_INPUT: number;
    GANTI_PAKET: number;
    ALAMAT_TIDAK_DITEMUKAN: number;

    SALAH_TAGGING: number;
    ODP_RUSAK: number;
    ODP_FULL: number;
    TIANG: number;
    CROSS_JALAN: number;
    TIDAK_ADA_ODP: number;
    ODP_JAUH: number;
    ODP_BELUM_GO_LIVE: number;
    ODP_RETI: number;
    LIMITASI_ONU: number;
    JALUR_RUTE: number;
    KENDALA_IKR_IKG: number;
    wo_3: number;
    wo_3_per: string;
    wo_4_7: number;
    wo_4_7_per: string;
    wo_8_14: number;
    wo_8_14_per: string;
    wo_15_30: number;
    wo_15_30_per: string;
    wo_gt_30: number;
    wo_gt_30_per: string;
    total_wo: number;
    ioreps_event_date: string;
}

export type DemandsDeploymentResponseData = {
    name: string | null;
    level: string;

    used_black: number;
    used_red: number;
    used_yellow: number;
    used_green: number;
    used: number;

    used_black_m1: number;
    used_red_m1: number;
    used_yellow_m1: number;
    used_green_m1: number;
    used_m1: number;

    avai_port_black: number;
    avai_port_red: number;
    avai_port_yellow: number;
    avai_port_green: number;
    avai_port: number;

    amount_port_black: number;
    amount_port_red: number;
    amount_port_yellow: number;
    amount_port_green: number;
    amount_port: number;

    amount_port_black_m1: number;
    amount_port_red_m1: number;
    amount_port_yellow_m1: number;
    amount_port_green_m1: number;
    amount_port_m1: number;

    total_odp_black: number;
    total_odp_red: number;
    total_odp_yellow: number;
    total_odp_green: number;
    total_odp: number;

    golive_m: number;
    golive_m1: number;
    golive_mom: string;
    golive_y: number;
    golive_y1: number;
    golive_yoy: string;
    golive_ytd: number;
    golive_ytd1: number;
    golive_ytd_per: string;

    target_ytd_demand: number;
    demand_created_mtd: number;
    demand_created_m1: number;
    demand_created_mom: string;
    ach_demands: string;

    avai_port_1mo_y: number;
    avai_port_2mo_y: number;
    avai_port_3mo_y: number;
    avai_port_4mo_y: number;

    used_1mo_y: number;
    used_2mo_y: number;
    used_3mo_y: number;
    used_4mo_y: number;
    used_gt_6mo_y: number;
    used_all_mo_y: number;

    amount_port_1mo_y: number;
    amount_port_2mo_y: number;
    amount_port_3mo_y: number;
    amount_port_4mo_y: number;
    amount_port_gt_6mo_y: number;
    amount_port_all_mo_y: number;

    used_1mo_y1: number;
    used_2mo_y1: number;
    used_3mo_y1: number;
    used_4mo_y1: number;
    used_gt_6mo_y1: number;
    used_all_mo_y1: number;

    amount_port_1mo_y1: number;
    amount_port_2mo_y1: number;
    amount_port_3mo_y1: number;
    amount_port_4mo_y1: number;
    amount_port_gt_6mo_y1: number;
    amount_port_all_mo_y1: number;

    occ_1mo_y: string;
    occ_2mo_y: string;
    occ_3mo_y: string;
    occ_4mo_y: string;
    occ_gt_6mo_y: string;
    occ_all_mo_y: string;

    occ_1mo_y1: string;
    occ_2mo_y1: string;
    occ_3mo_y1: string;
    occ_4mo_y1: string;
    occ_gt_6mo_y1: string;
    occ_all_mo_y1: string;

    occ_1mo_2y: string;
    occ_2mo_2y: string;
    occ_3mo_2y: string;
    occ_4mo_2y: string;
    occ_gt_6mo_2y: string;
    occ_all_mo_2y: string;
}

export type RevenueC3mrResponseData = {
    name: string;
    target_rev_all: number;
    bill_amount_all: number;
    bill_amount_all_unpaid: number;
    ach_fm_rev_all: string;
    gap_to_target_rev_all: number;
    target_rev_ns: number;
    bill_amount_ns: number;
    bill_amount_ns_unpaid: number;
    ach_fm_rev_ns: string;
    gap_to_target_rev_ns: number;
    target_rev_existing: number;
    bill_amount_existing: number;
    bill_amount_existing_unpaid: number;
    ach_fm_rev_existing: string;
    gap_to_target_rev_existing: number;
    subs_0_6: number;
    subs_paid_0_6: number;
    ach_subs_0_6: string;
    subs_gt_6: number;
    subs_paid_gt_6: number;
    ach_subs_paid_gt_6: string;
    revenue_loss: number;
}