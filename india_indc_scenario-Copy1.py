#!/usr/bin/env python
# coding: utf-8

# In[1]:


import itertools

import numpy as np
import pandas as pd
from itertools import product

from ixmp import Platform
import message_ix
import xlsx_core

mp = Platform(dbtype='HSQLDB')


# # Set options

# In[ ]:


# Choose whether or not to save figures as pdf files
save = False
# Choose whether or not to add constraint for INDC target 40% share RE electricity capacity
indc_constraint = True
# Choose whether to use a carbon price on GHG emissions (if a price should be used, set price = X$/Mt CO2e)
price = False
# Choose whether to write plotted data to xlsx file
output_xlsx = 'West_Bengal_Scenario_EV'


# # Open Baseline and Clone

# In[ ]:


# Creates a new data structure based on model and scenario name
model = "West Bengal energy model"
scen = "baseline"
ds_to_clone = mp.Scenario(model, scen, cache=True)
ds = ds_to_clone.clone(model, 'EV_test_Scenario', keep_sol=False)
ds.check_out()


# In[ ]:


# Scenario variation - EV Test
def CAGR(first, last, periods):
            vals = (last / first)**(1 / periods)-1
#             vals = vals.rename(last.name)
            return vals
mp.close_db()


# In[ ]:


elec_p=0.30
gas_p=0.10
oil_p=0.60
target_yr='2020'
total_p=elec_p+oil_p+gas_p

tec_R_Large=['elec_LargeP_road','oil_LargeP_road','gas_LargeP_road']
tec_R_Small=['elec_SmallP_road','oil_SmallP_road','gas_SmallP_road']

road_p_L=xlsx_core.apply_filters(tecs, filters={'Technology':tec_R_Large,'Parameter':['bound_activity_lo'], 'Units': ['bvkm']})
road_p_S=xlsx_core.apply_filters(tecs, filters={'Technology':tec_R_Small,'Parameter':['bound_activity_lo'], 'Units': ['bvkm']})
large_v=xlsx_core.apply_filters(tecs, filters={'Technology':'large_vehicle','Parameter':['bound_activity_lo'], 'Units': ['bpkm']})
small_v=xlsx_core.apply_filters(tecs, filters={'Technology':'small_vehicle','Parameter':['bound_activity_lo'], 'Units': ['bpkm']})

large_blo_s=road_p_L.groupby(['Parameter']).sum()
small_blo_s=road_p_S.groupby(['Parameter']).sum()

#Calculation of Share at Target Year
fn_yr_dem=small_blo_s[horizon[-1]]
change_R=[elec_p*float(fn_yr_dem),gas_p*float(fn_yr_dem),oil_p*float(fn_yr_dem)]
print(change_R)

#Calculation of CAGR from target year to final year
cagr_val=pow((CAGR(road_p_S[target_yr].values,change_R,(int(horizon[-1])-int(target_yr)))+1),5)
print(cagr_val)

for year in [y for y in horizon if y > target_yr]:
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
        ds.add_par("bound_activity_lo",par)
        ds.add_par("bound_activity_up",par)
    


# In[ ]:





# # Solve

# In[ ]:


comment = 'WB_test_scenario'
ds.commit(comment)
ds.set_as_default()


# In[ ]:


ds.solve(model='MESSAGE')


# In[ ]:


# calc= ds.par("bound_activity_lo").value*ds.par("emission_factor").
# Adds emission factors for technologies
import xlsx_core
ds.add_set('emission', 'GHG')
ds.add_cat('emission', 'GHG', 'GHG')
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


# # Postprocessing

# In[ ]:


import plots
get_ipython().run_line_magic('matplotlib', 'inline')

# Load xlsx sheet required for postprocessing
fname = 'West Bengal energy model_baseline.xlsx'
dfs = pd.ExcelFile(fname)
tecs = dfs.parse('technologies')

figures = plots.figures(ds, tecs, save, output_xlsx)


# In[ ]:


figures.plot_all()


# In[ ]:


mp.close_db()


# In[ ]:





