#!/usr/bin/env python
# coding: utf-8

# # Set options

# In[1]:


# Enter name of the input file that should be read
fname = 'wb_baseline_EAP.xlsx'
# Choose whether or not data entered into the datastrucuture should be displayed (True or False)
verbose = False
# Choose whether or not data input errors are shown (True or False)
disp_error = False
# Choose whether or not to add constraint for INDC target 40% share RE electricity capacity
supply_constraint = True
# Choose whether to use a carbon price on GHG emissions (if a price should be used, set price = X$/Mt CO2e)
price = False
#price = 10000000.
# Choose whether to write plotted data to xlsx file
output_xlsx = True
# Choose whether to introduce mpas for final-to-useful technologies. 
# To activate, add the year for which an mpa should be generated. This should not be equal to a year which is calibtrated
#mpa_gen = 2025
mpa_gen = False
# Choose whether to include soft_constraints on mpa/mpc lo/up
soft_constraints = True
# Choose whether to add share of gas_cc/gas_cc_ccs peak-load production 
gas_peak_load_share = False
# Choose whether to add inconvenience costs
inconvenience_costs = False
#Input the percentage of Electrical Vehicle pentration required in 2040. Use False to exclude the EV_Penetration
elec_veh_pen= 0.50
#Input the percentage of Industrial Thermal shift from coal to electricity required in 2040.
#Use False to exclude the scenario
industry_elec_thermal=False
#Electric Cooking pentration
elec_cook=0.45
#lpg cooking penetration
lpg_cook=0.50
#Industry Specific efficiency penetration
industrial_efficiency= True
#passenger IWT penetration
passenger_IWT=True
#Freight IWT penetration
freight_IWT=True
#solar thermal penetration
Solar_thermal=True
#Distribution Efficiency
dist_efficiency= True
#RPO
RPO_constraint=False


# # Load packages

# In[2]:


import itertools
from itertools import product
import numpy as np
import pandas as pd
import message_ix
from ixmp import Platform
mp = Platform(dbtype='HSQLDB')

import xlsx_core
im = xlsx_core.init_model(mp, fname, verbose, disp_error)


# # Read in input data

# In[3]:


meta, tecs, dems, resources, mpa_data = im.read_input()


# # Create scenario

# In[4]:


scenario, model_nm, scen_nm = im.create_scen()


# # Setup scenario metadata

# In[5]:


horizon, vintage_years, firstyear = im.add_metadata()


# # Process input data

# ## Import class add_par from xlsx_core

# In[6]:


ap = xlsx_core.add_par(scenario, horizon, vintage_years, firstyear, disp_error)


# ## Process demand data

# In[7]:


im.demand_input_data(ap)


# ## Process fossil resource data

# In[8]:


im.fossil_resource_input_data(ap)


# ## Process technology data

# In[9]:


im.technology_input_data(ap)


# ## Process renewable resource data

# In[10]:


im.renewable_resource_input_data(ap)


# # Custom scenario functions

# ## Insert mpas for final-to-useful technologies taking into account the demand development tractory

# In[11]:


if mpa_gen:
     im.final_energy_mpa(mpa_gen)
# mpa_data


# ## Useful share constraints

# In[12]:


share = xlsx_core.apply_filters(tecs, filters={'Parameter': [p for p in tecs['Parameter'].dropna().unique().tolist() if 'share' in p]})
ap.add_upper_share(share)


# ## Add soft constraints for mpa lo/up and mpc lo/up

# In[13]:


if soft_constraints:
    # For growth_activity and growth_new_capacity constraints, define [<% relaxation>,<% of LCOE>]
    # Note that these are applied to all technologies with growth_activity or growth_new_capacity constraints
    mpalo = [-0.05,0.5]
    mpaup = [0.05,0.5]
    mpclo = [-0.05,0.5]
    mpcup = [0.05,0.5]
    im.rel_soft_constraints(mpalo=mpalo,mpaup=mpaup,mpclo=mpclo,mpcup=mpcup)


# ## Add inconvenience costs

# In[14]:


if inconvenience_costs:
    im.inconvenience_costs()


# ## Insert gas peak share

# In[15]:


if gas_peak_load_share:
    share = .05
    elec_tecs = scenario.par('output', filters={'level': ['secondary'], 'commodity': ['electricity']})
    elec_tecs = elec_tecs.drop(['commodity','level','node_dest','time_dest','year_vtg','time'], axis=1).drop_duplicates()
    elec_tecs = elec_tecs[elec_tecs['year_act'] > 2020]
    yrs = elec_tecs['year_act'].unique().tolist()
    rhs = elec_tecs[~elec_tecs['technology'].isin(['gas_cc_peak_ppl'])]
    lhs = elec_tecs[elec_tecs['technology'].isin(['gas_cc_peak_ppl'])]

    scenario.add_set("relation", "gas_cc_peakload_share")
    par = pd.DataFrame({
                    'relation': 'gas_cc_peakload_share',
                    'node_rel': 'India',
                    'year_rel': yrs,
                    'unit': '%',
                    'value': 0.0
                    })
    scenario.add_par("relation_lower", par)

    rhs.loc[:,'year_rel'] = rhs.loc[:,'year_act']
    rhs.loc[:,'node_rel'] = rhs.loc[:,'node_loc']
    rhs.loc[:,'relation'] = 'gas_cc_peakload_share'
    rhs.loc[:,'value'] = -1.0
    
    scenario.add_par("relation_activity", rhs)
    
    lhs.loc[:,'year_rel'] = lhs.loc[:,'year_act']
    lhs.loc[:,'node_rel'] = lhs.loc[:,'node_loc']
    lhs.loc[:,'relation'] = 'gas_cc_peakload_share'
    lhs.loc[:,'value'] = (1.-share)/share
    
    scenario.add_par("relation_activity", lhs)


# ## Share of Supply Constraint

# In[16]:


if supply_constraint:
#    rhs = ['coal_ppl','coal_ppl_sub','coal_usc','coal_usc_ccs','dg_set','gas_cc_ccs_ppl','gas_cc_ppl','igcc','igcc_ccs','nuc_ppl']
    rhs = ['coal_ppl','coal_ppl_sub']
    lhs = ['bio_ppl','hydro','solar_PV','solar_RPO_offgrid','solar_RPO']

    scenario.add_set("relation", "se_elec_nf_share")
    par = pd.DataFrame({
                    'relation': 'se_elec_nf_share',
                    'node_rel': 'India',
                    'year_rel': '2030',
                    'unit': '%',
                    'value': [0.0]
                    })
    scenario.add_par("relation_lower", par)

    share = .20

    for tec in rhs:
        par = pd.DataFrame({
                        'relation': 'se_elec_nf_share',
                        'node_rel': 'India',   
                        'year_rel': '2030',
                        'year_act': '2035',
                        'mode': 'standard',
                        'node_loc': 'India',
                        'technology': tec,
                        'unit': '%',
                        'value': [-1.0]
                        })
        scenario.add_par("relation_activity", par)

    for tec in lhs:
        par = pd.DataFrame({
                        'relation': 'se_elec_nf_share',
                        'node_rel': 'India',   
                        'year_rel': '2030',
                        'year_act': '2035',
                        'mode': 'standard',            
                        'node_loc': 'India',
                        'technology': tec,
                        'unit': '%',
                        'value': [(1.-share)/share]
                        #'value': [2.1]
                        })
        scenario.add_par("relation_activity", par)
    


# In[17]:


if RPO_constraint:
#    rhs = ['coal_ppl','coal_ppl_sub','coal_usc','coal_usc_ccs','dg_set','gas_cc_ccs_ppl','gas_cc_ppl','igcc','igcc_ccs','nuc_ppl']
    rhs = ['coal_ppl','coal_ppl_sub','bio_ppl','hydro','solar_PV']
    lhs = ['solar_RPO_offgrid','solar_RPO']

    scenario.add_set("relation", "se_RPO_share")
    par = pd.DataFrame({
                    'relation': 'se_RPO_share',
                    'node_rel': 'India',
                    'year_rel': '2030',
                    'unit': '%',
                    'value': [0.0]
                    })
    scenario.add_par("relation_lower", par)

    share = .10

    for tec in rhs:
        par = pd.DataFrame({
                        'relation': 'se_RPO_share',
                        'node_rel': 'India',   
                        'year_rel': '2030',
                        'year_act': '2030',
                        'mode': 'standard',
                        'node_loc': 'India',
                        'technology': tec,
                        'unit': '%',
                        'value': [-1.0]
                        })
        scenario.add_par("relation_activity", par)

    for tec in lhs:
        par = pd.DataFrame({
                        'relation': 'se_RPO_share',
                        'node_rel': 'India',   
                        'year_rel': '2030',
                        'year_act': '2030',
                        'mode': 'standard',            
                        'node_loc': 'India',
                        'technology': tec,
                        'unit': '%',
                        'value': [(1.-share)/share]
                        #'value': [2.1]
                        })
        scenario.add_par("relation_activity", par)


# In[18]:


if industrial_efficiency:
    ind_dat=scenario.par("input")[scenario.par("input").technology=="elec_ind-specific"]
    yrs=[2020,2025,2030,2035,2040]
    in_indus=[1.29870130,1.28205128,1.25000000,1.21951220,1.20481928]
    for i in range(len(yrs)):
                   ind_dat.value[ind_dat.year_vtg==yrs[i]]=in_indus[i]
    scenario.add_par('input',ind_dat)
if dist_efficiency:
    dist_dat=scenario.par("input")[scenario.par("input").technology=="elec_grid"]
    yrs=[2020,2025,2030,2035,2040]
    in_dist=[1.2900000,1.2600000,1.2300000,1.2000000,1.1800000]
    for i in range(len(yrs)):
                   dist_dat.value[dist_dat.year_vtg==yrs[i]]=in_dist[i]
    scenario.add_par('input',dist_dat)


# ## Add Electrical Vehichle Penetration

# In[19]:


if elec_veh_pen:
    elec_p=elec_veh_pen
    def CAGR_cal(first, last, periods):
            vals = (last / first)**(1 / periods)-1
            return vals

    
    gas_p=0.05 #keep minimum to avoid infeasibility
    oil_p=1.-gas_p-elec_p
    pen_yr='2020'
#     print(elec_p,oil_p,gas_p)
    
    tec_R_Small=['elec_SmallP_road','oil_SmallP_road','gas_SmallP_road']
    road_p_S=xlsx_core.apply_filters(tecs, filters={'Technology':tec_R_Small,'Parameter':['bound_activity_lo'], 'Units': ['bvkm']})
#     large_v=xlsx_core.apply_filters(tecs, filters={'Technology':'large_vehicle','Parameter':['bound_activity_lo'], 'Units': ['bpkm']})
#     small_v=xlsx_core.apply_filters(tecs, filters={'Technology':'small_vehicle','Parameter':['bound_activity_lo'], 'Units': ['bpkm']})
    small_blo_s=road_p_S.groupby(['Parameter']).sum()
    #Check Wether the technology is in the model
    if gas_p>0.01:
        if (road_p_S[road_p_S.Technology=="gas_SmallP_road"][pen_yr].values==0.0):
            road_p_S[pen_yr][road_p_S.Technology=="gas_SmallP_road"]=0.5
            
    #Calculation of Share at Target Year
    fn_yr_dem=small_blo_s[horizon[-1]]
    change_R=[elec_p*float(fn_yr_dem),gas_p*float(fn_yr_dem),oil_p*float(fn_yr_dem)]
#     print(change_R)

    #Calculation of CAGR from target year to final year
    cagr_val=pow((CAGR_cal(road_p_S[pen_yr].values,change_R,(int(horizon[-1])-int(pen_yr)))+1),5)
#     print(cagr_val)

    for year in [y for y in horizon if y > pen_yr]:
        road_p_S.loc[road_p_S.index,year]=road_p_S.loc[road_p_S.index,str(int(year)-5)]*cagr_val
        for i in road_p_S.index:
            par = pd.DataFrame({
                        'technology': road_p_S.Technology[i],
                        'node_loc': 'India',
                        'year_act': [year],
                        'time': 'year',
                        'mode':'standard',
                        'unit': 'bvkm',
                        'value': [road_p_S.loc[i,year]]
                        })
            if road_p_S.Technology[i]=="gas_SmallP_road":
                scenario.add_par("bound_activity_lo",par.fillna(0))
                scenario.add_par("bound_activity_up",par.fillna(0))
            else:
                scenario.add_par("bound_activity_lo",par)      
#                 scenario.add_par("bound_activity_lo",par.fillna(0))
#                 scenario.add_par("bound_activity_up",par)


# In[20]:


if passenger_IWT:
    IWT_dat=scenario.par("demand")[scenario.par("demand").commodity=="p_transport_IWT"]
    road_dem=scenario.par("demand")[scenario.par("demand").commodity=="p_transport_road"]
    rail_dat=scenario.par("demand")[scenario.par("demand").commodity=="p_transport_rail"]
    road_dat=scenario.par("bound_activity_up")[scenario.par("bound_activity_up").technology=="large_vehicle"]
    yrs=[2020,2025,2030,2035,2040]#Years of modal share
    IWT_Share=[0.05,0.1,0.15,0.20,0.25]#Modal Share of IWT
    rail_Share=[0.419,0.439,0.459,0.480,0.505] #Modal Share of rail
    road_Share=[len(yrs)]
    road_Share[:] = [1.00- x for x in (np.array(IWT_Share)+np.array(rail_Share))]
    sum_bpkm=IWT_dat.value.values+road_dat.value.values+rail_dat.value.values
    for i in range(len(yrs)):
        tmp=IWT_dat.value[IWT_dat.year==yrs[i]].values
        tmp_dem=road_dem.value[road_dem.year==yrs[i]].values
        IWT_dat.value[IWT_dat.year==yrs[i]]=IWT_Share[i]*sum_bpkm[i+1]
        road_dem.value[road_dem.year==yrs[i]]=tmp_dem[0]-abs(IWT_dat.value[IWT_dat.year==yrs[i]].values[0]-tmp[0])
        road_dat.value[road_dat.year_act==yrs[i]]=road_Share[i]*sum_bpkm[i+1]
        rail_dat.value[rail_dat.year==yrs[i]]=rail_Share[i]*sum_bpkm[i+1]
    scenario.add_par('demand',IWT_dat)
    scenario.add_par('demand',road_dem)
    scenario.add_par('demand',rail_dat)
    scenario.add_par('bound_activity_up',road_dat)
if freight_IWT:
    IWTF_dat=scenario.par("demand")[scenario.par("demand").commodity=="f_transport_IWT"]
    roadF_dat=scenario.par("demand")[scenario.par("demand").commodity=="f_transport_road"]
    railF_dat=scenario.par("demand")[scenario.par("demand").commodity=="f_transport_rail"]
    yrs=[2020,2025,2030,2035,2040]#Years of modal share
    IWTF_Share=[0.05,0.12,0.19,0.26,0.33]#Modal Share of IWT
    railF_Share=[0.218,0.224,0.230,0.237,0.244] #Modal Share of rail
    roadF_Share=[len(yrs)]
    roadF_Share[:] = [1.00- x for x in (np.array(IWTF_Share)+np.array(railF_Share))]
    sum_btkm=IWTF_dat.value.values+roadF_dat.value.values+railF_dat.value.values
    for i in range(len(yrs)):
        IWTF_dat.value[IWTF_dat.year==yrs[i]]=IWTF_Share[i]*sum_btkm[i+1]
        roadF_dat.value[roadF_dat.year==yrs[i]]=roadF_Share[i]*sum_btkm[i+1]
        railF_dat.value[railF_dat.year==yrs[i]]=railF_Share[i]*sum_btkm[i+1]
    scenario.add_par('demand',IWTF_dat)
    scenario.add_par('demand',roadF_dat)
    scenario.add_par('demand',railF_dat)


# In[21]:


# if True:
#     pumps_dat=scenario.par("bound_activity_lo")[scenario.par("bound_activity_lo").technology=="oil_agri-pump"]
if Solar_thermal:
    sol_heat_res=0.20
    sol_heat_comm=0.40
    elec_heat_res=1.-sol_heat_res
    elec_heat_comm=1.-sol_heat_comm
    def CAGR_cal(first, last, periods):
            vals = (last / first)**(1 / periods)-1
            return vals
    pen_yr='2020'
#     print(elec_ind,coal_ind)
    heat_tec=['elec_comm-HW','elec_res-HW']
    demand_tec=['commercial_hotwater','residential_hotwater']
    heat_data=xlsx_core.apply_filters(tecs, filters={'Technology':heat_tec,'Parameter':['bound_activity_lo'], 'Units': ['GWa']})
    dem_heat=xlsx_core.apply_filters(dems, filters={'Variable':demand_tec,'Parameter':['demand'], 'Units': ['GWa']})
#     ind_thm_s=ind_thm.groupby(['Parameter']).sum()
#     print(dem_heat)
#     print(heat_data)

    #Calculation of Share at Target Year
    fn_yr_dem=dem_heat[horizon[-1]].values
#     print(fn_yr_dem)
    change_R=[elec_heat_comm*float(fn_yr_dem[0]),elec_heat_res*float(fn_yr_dem[1])]
#     print(change_R)

    #Calculation of CAGR from target year to final year
    cagr_val=pow((CAGR_cal(heat_data[pen_yr].values,change_R,(int(horizon[-1])-int(pen_yr)))+1),5)
#     print(cagr_val)

    for year in [y for y in horizon if y > pen_yr]:
        heat_data.loc[heat_data.index,year]=heat_data.loc[heat_data.index,str(int(year)-5)]*cagr_val
        for i in heat_data.index:
            par = pd.DataFrame({
                        'technology': heat_data.Technology[i],
                        'node_loc': 'India',
                        'year_act': [year],
                        'time': 'year',
                        'mode':'standard',
                        'unit': 'GWa',
                        'value': [heat_data.loc[i,year]]
                        })
#             print(par)
            scenario.add_par("bound_activity_lo",par)
# #             scenario.add_par("bound_activity_up",par)


# In[22]:


if industry_elec_thermal:
    elec_ind=industry_elec_thermal
    def CAGR_cal(first, last, periods):
            vals = (last / first)**(1 / periods)-1
            return vals
    coal_ind=1.-elec_ind
    pen_yr='2020'
#     print(elec_ind,coal_ind)
    tec_ind=['coal_ind-thermal','elec_ind-thermal']
    ind_thm=xlsx_core.apply_filters(tecs, filters={'Technology':tec_ind,'Parameter':['bound_activity_lo'], 'Units': ['GWa']})
    ind_thm_s=ind_thm.groupby(['Parameter']).sum()
#     print(ind_thm_s)

    #Calculation of Share at Target Year
    fn_yr_dem=ind_thm_s[horizon[-1]]
    change_R=[elec_ind*float(fn_yr_dem),coal_ind*float(fn_yr_dem)]
#     print(change_R)

    #Calculation of CAGR from target year to final year
    cagr_val=pow((CAGR_cal(ind_thm[pen_yr].values,change_R,(int(horizon[-1])-int(pen_yr)))+1),5)
#     print(cagr_val)

    for year in [y for y in horizon if y > pen_yr]:
        ind_thm.loc[ind_thm.index,year]=ind_thm.loc[ind_thm.index,str(int(year)-5)]*cagr_val
        for i in ind_thm.index:
            par = pd.DataFrame({
                        'technology': ind_thm.Technology[i],
                        'node_loc': 'India',
                        'year_act': [year],
                        'time': 'year',
                        'mode':'standard',
                        'unit': 'GWa',
                        'value': [ind_thm.loc[i,year]]
                        })
#             print(par)
            scenario.add_par("bound_activity_lo",par)
#             scenario.add_par("bound_activity_up",par)


# In[23]:


if elec_cook:
#     elec_cook=0.20 #electric cooking percentage share
#     lpg_cook=0.65 #LPG/Gas cooking percentage share
#     oil_cook=0.00 #Oil cooking percentage share
    bio_cook_share=1.-elec_cook-lpg_cook #Biomass based cooking percentage share including non-commercial and modern chullha.
    #Percentage Share of Bio Cooking for Traditional and Modern Biomass
    mod_cook_share=0.50
    trad_cook_share=1.-mod_cook_share
    #Split the share
    bio_cook=mod_cook_share*bio_cook_share
    trad_cook=trad_cook_share*bio_cook_share
#     print(trad_cook,bio_cook)
    
    def CAGR_cal(first, last, periods):
            vals = (last / first)**(1 / periods)-1
            return vals

    pen_yr='2020'
#     print(elec_cook,lpg_cook,gas_p)
    tec_cooking=['bio_cooking','elec_cooking','gas_cooking','traditional chullah']
    

    cooking_dat=xlsx_core.apply_filters(tecs, filters={'Technology':tec_cooking,'Parameter':['bound_activity_lo'], 'Units': ['GWa']})
    cooking_sum=cooking_dat.groupby(['Parameter']).sum()
#     print(cooking_dat)
#     print(cooking_sum)
    #Calculation of Share at Target Year
    fn_yr_dem=cooking_sum[horizon[-1]]
    change_C=[bio_cook*float(fn_yr_dem),elec_cook*float(fn_yr_dem),lpg_cook*float(fn_yr_dem),trad_cook*float(fn_yr_dem)]
#     print(change_C)

    #Calculation of CAGR from target year to final year
    cagr_val=pow((CAGR_cal(cooking_dat[pen_yr].values,change_C,(int(horizon[-1])-int(pen_yr)))+1),5)
#     print(cagr_val)

    for year in [y for y in horizon if y > pen_yr]:
        cooking_dat.loc[cooking_dat.index,year]=cooking_dat.loc[cooking_dat.index,str(int(year)-5)]*cagr_val
        for i in cooking_dat.index:
            par = pd.DataFrame({
                        'technology': cooking_dat.Technology[i],
                        'node_loc': 'India',
                        'year_act': [year],
                        'time': 'year',
                        'mode':'standard',
                        'unit': 'GWa',
                        'value': [cooking_dat.loc[i,year]]
                        })
#             print(par)
            if cooking_dat.Technology[i]=="traditional chullha":
                scenario.add_par("bound_activity_lo",par)
                scenario.add_par("bound_activity_up",par)
            else:
                scenario.add_par("bound_activity_lo",par)


# ## Add GHG emission accounting using AR4 GWP CH4 (*25) and N2O (*298)

# In[24]:


# Adds emission factors for technologies
scenario.add_set('emission', 'GHG')
scenario.add_cat('emission', 'GHG', 'GHG')
ghg = xlsx_core.apply_filters(tecs, filters={'Parameter':'emission_factor', 'Species': ['CO2','CH4','N2O']})
if not ghg.empty:
#    ghg_conversion = {'CH4': 25, 'N2O': 298}
    ghg_conversion = {'CH4': 1, 'N2O': 1}    
    for s in ghg_conversion:
        tmp = ghg[ghg['Species'] == s]
        for year in [y for y in horizon if y in tmp.columns]:
            tmp[year] = tmp[year] * ghg_conversion[s]
        ghg = tmp.combine_first(ghg)
    ghg = ghg.groupby(['Technology', 'Parameter', 'Region', 'Mode', 'Units']).sum()
    ghg['Species'] = 'GHG'
    ghg = ghg.reset_index()
    ap.add_tec_emi_fac(ghg)


# ## Add Carbon price (in INR per MtCO2e/yr)

# In[25]:


if price:
    if type(price) != float:
        print('Please ensure that the price is specified as a float')
    else:
        unit = 'USD/MtCO2e'
        if unit not in mp.units():
            mp.add_unit(unit, comment="Adding new unit required for emission tax")
        years = [y for y in ds.set("year") if int(y) >= ds.set("cat_year", filters={"type_year": ['firstmodelyear']})['year'][0]]
        vals = []
        for y in years:
            if y not in scenario.set('type_year'):
                scenario.add_set('type_year', y)
            if y == '2015':
                val = price
            else:
                val = val * pow(scenario.par("interestrate", filters={'year': ['2015']})['value'].values + 1,(float(y) - float(years[years.index(y)-1])))
            vals.append(val)
            
        par = pd.DataFrame({
            'node': 'India',
            'type_emission': 'GHG',
            'type_tec': 'all',
            'type_year': years,
            'unit': unit,
            'value': vals
        })
        #print(par)
        scenario.add_par('tax_emission', par)


# # Solve

# In[26]:


comment = 'WB-India baseline scenario'
scenario.commit(comment)
scenario.set_as_default()


# In[27]:


scenario.solve(model='MESSAGE')


# # Postprocessing

# ## Run IAMC reporting

# In[28]:


import os
import sys
# Retrieve MESSAGE_DATA_PATH
msg_data_path = os.environ['MESSAGE_DATA_PATH']
# Set reporting path
reporting_path = '{}\\post-processing\\reporting'.format(msg_data_path)
sys.path.append(reporting_path)
from iamc_report_india import report as reporting

reporting(mp, scenario, 'False', model_nm, scen_nm, merge_hist=True)


# ## Create plots using pyam

# In[29]:


import india_plots
get_ipython().run_line_magic('matplotlib', 'inline')

# Select whether to use unit conversion
unit_conv = False
# Select whether to rename variables
rename = True
# Select whether to limit time_horzizon for which data is shown select
# set to either False (turn off functionality) or
# set [<year start>, <year end>]
plot_years = [2015,2040]
# plot_years = False
# Choose whether or not to save figures as pdf files
save = True
# Choose wich configuration file to use
config = '{}\\runscript\\india_plot_config.yml'.format(msg_data_path)
# Load plotting functions
plots = india_plots.plot_results(scenario, model_nm, scen_nm, unit_conv, rename, plot_years, save, config)


# In[30]:


# Plots related to resources
plots.resource_extr()
plots.resource_extr_cum()
# Plots related to Primary Energy
plots.pe_source()
plots.pe_source_share()
# Plots related to Secondary Energy
plots.se_elec_source()
plots.se_elec_source_share()
plots.se_elec_source_tic()
plots.se_elec_source_nic()
plots.se_gases_source()
plots.se_solids_source()
plots.se_elec_source_inv()
plots.se_elec_source_inv_res()
# Plots related to Prices
plots.pe_prices()
plots.fe_prices()
plots.crb_price()
# Plots related to Demands
# plots.demands_sector()
# Fuel use across FE sectors
plots.fe_elec_sector()
# Plots related to Final Energy
# plots.demands_sector_input()
plots.fe_ResTot_source()
plots.fe_ResCook_source()
plots.fe_ResHW_source()
plots.fe_ResOth_source()
plots.fe_CommTot_source()
plots.fe_IndTot_source()
plots.fe_IndSpec_source()
plots.fe_IndTherm_source()
plots.fe_TrpTot_source()
plots.fe_TrpPas_source()
plots.fe_TrpFrt_source()
plots.fe_CommHW_source()
plots.fe_CommOth_source()
plots.fe_AgriTot_source()
# Plots related to CO2 emissions
plots.CO2emi_Sequestration_source()
plots.CO2emi_sector()
plots.CO2emi_Sup_sector()
plots.CO2emi_Sup_Elec_source()
plots.CO2emi_Dem_source()
plots.CO2emi_Dem_Res_source()
plots.CO2emi_Dem_AFOFI_source()
plots.CO2emi_Dem_TRP_Freight_sector()
plots.CO2emi_Dem_TRP_Passenger_sector()
plots.CO2emi_Dem_TRP_Tot_sector()
#plots.CO2emi_Other_sector()


# In[31]:


mp.close_db()


# In[32]:


dir(scenario)


# In[33]:


scenario.clone()


# In[ ]:




