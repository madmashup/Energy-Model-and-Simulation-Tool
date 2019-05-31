from flask import Flask, render_template, request
from os import path
from io import BytesIO
import base64
import operator

app = Flask(__name__)

@app.route('/scenario', methods=['POST'])

gas_peak_load_share = request.form['gas_peak_load_share']
inconvenience_costs = request.form['inconvenience_costs']
elec_veh_pen = request.form['elec_veh_pen']
industry_elec_thermal = request.form['industry_elec_thermal']
elec_cook = request.form['elec_cook']
lpg_cook = request.form['lpg_cook']
industrial_efficiency = request.form['industrial_efficiency']
passenger_IWT = request.form['passenger_IWT']
freight_IWT = request.form['freight_IWT']
Solar_thermal = request.form['Solar_thermal']
dist_efficiency = request.form['dist_efficiency']
RPO_constraint = request.form['RPO_constraint']
share_constraints = request.form['share_constraints']

def scen(gas_peak_load_share, inconvenience_costs, elec_veh_pen, industry_elec_thermal, elec_cook, lpg_cook, industrial_efficiency, passenger_IWT, freight_IWT, Solar_thermal, dist_efficiency, RPO_constraint, share_constraints):

    # Creates a new data structure based on model and scenario name
    model = "West Bengal energy model"
    scen = "baseline"
    ds_to_clone = mp.Scenario(model, scen, cache=True)
    ds = ds_to_clone.clone(model, 'test_Scenario', keep_sol=False)
    ds.check_out()

    # ## Add inconvenience costs
    if inconvenience_costs:
        im.inconvenience_costs()
    # ## Insert gas peak share
    if gas_peak_load_share:
        share = .05
        elec_tecs = ds.par('output', filters={'level': ['secondary'], 'commodity': ['electricity']})
        elec_tecs = elec_tecs.drop(['commodity','level','node_dest','time_dest','year_vtg','time'], axis=1).drop_duplicates()
        elec_tecs = elec_tecs[elec_tecs['year_act'] > 2020]
        yrs = elec_tecs['year_act'].unique().tolist()
        rhs = elec_tecs[~elec_tecs['technology'].isin(['gas_cc_peak_ppl'])]
        lhs = elec_tecs[elec_tecs['technology'].isin(['gas_cc_peak_ppl'])]

        ds.add_set("relation", "gas_cc_peakload_share")
        par = pd.DataFrame({
                        'relation': 'gas_cc_peakload_share',
                        'node_rel': 'India',
                        'year_rel': yrs,
                        'unit': '%',
                        'value': 0.0
                        })
        ds.add_par("relation_lower", par)

        rhs.loc[:,'year_rel'] = rhs.loc[:,'year_act']
        rhs.loc[:,'node_rel'] = rhs.loc[:,'node_loc']
        rhs.loc[:,'relation'] = 'gas_cc_peakload_share'
        rhs.loc[:,'value'] = -1.0

        ds.add_par("relation_activity", rhs)

        lhs.loc[:,'year_rel'] = lhs.loc[:,'year_act']
        lhs.loc[:,'node_rel'] = lhs.loc[:,'node_loc']
        lhs.loc[:,'relation'] = 'gas_cc_peakload_share'
        lhs.loc[:,'value'] = (1.-share)/share

        ds.add_par("relation_activity", lhs)
    # ## Share of Supply Constraint
    if supply_constraint:
    #    rhs = ['coal_ppl','coal_ppl_sub','coal_usc','coal_usc_ccs','dg_set','gas_cc_ccs_ppl','gas_cc_ppl','igcc','igcc_ccs','nuc_ppl']
        rhs = ['coal_ppl','coal_ppl_sub']
        lhs = ['bio_ppl','hydro','solar_PV','solar_RPO_offgrid','solar_RPO']

        ds.add_set("relation", "se_elec_nf_share")
        par = pd.DataFrame({
                        'relation': 'se_elec_nf_share',
                        'node_rel': 'India',
                        'year_rel': '2030',
                        'unit': '%',
                        'value': [0.0]
                        })
        ds.add_par("relation_lower", par)

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
            ds.add_par("relation_activity", par)

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
            ds.add_par("relation_activity", par)

    if RPO_constraint:
    #    rhs = ['coal_ppl','coal_ppl_sub','coal_usc','coal_usc_ccs','dg_set','gas_cc_ccs_ppl','gas_cc_ppl','igcc','igcc_ccs','nuc_ppl']
        rhs = ['coal_ppl','coal_ppl_sub','bio_ppl','hydro','solar_PV']
        lhs = ['solar_RPO_offgrid','solar_RPO']

        ds.add_set("relation", "se_RPO_share")
        par = pd.DataFrame({
                        'relation': 'se_RPO_share',
                        'node_rel': 'India',
                        'year_rel': '2030',
                        'unit': '%',
                        'value': [0.0]
                        })
        ds.add_par("relation_lower", par)

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
            ds.add_par("relation_activity", par)

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
            ds.add_par("relation_activity", par)

    if industrial_efficiency:
        ind_dat=ds.par("input")[ds.par("input").technology=="elec_ind-specific"]
        yrs=[2020,2025,2030,2035,2040]
        in_indus=[1.29870130,1.28205128,1.25000000,1.21951220,1.20481928]
        for i in range(len(yrs)):
                       ind_dat.value[ind_dat.year_vtg==yrs[i]]=in_indus[i]
        ds.add_par('input',ind_dat)
    if dist_efficiency:
        dist_dat=ds.par("input")[ds.par("input").technology=="elec_grid"]
        yrs=[2020,2025,2030,2035,2040]
        in_dist=[1.2900000,1.2600000,1.2300000,1.2000000,1.1800000]
        for i in range(len(yrs)):
                       dist_dat.value[dist_dat.year_vtg==yrs[i]]=in_dist[i]
        ds.add_par('input',dist_dat)

    # ## Add Electrical Vehichle Penetration
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
                    ds.add_par("bound_activity_lo",par.fillna(0))
                    ds.add_par("bound_activity_up",par.fillna(0))
                else:
                    ds.add_par("bound_activity_lo",par)      
    #                 ds.add_par("bound_activity_lo",par.fillna(0))
    #                 ds.add_par("bound_activity_up",par)

    if passenger_IWT:
        IWT_dat=ds.par("demand")[ds.par("demand").commodity=="p_transport_IWT"]
        road_dem=ds.par("demand")[ds.par("demand").commodity=="p_transport_road"]
        rail_dat=ds.par("demand")[ds.par("demand").commodity=="p_transport_rail"]
        road_dat=ds.par("bound_activity_up")[ds.par("bound_activity_up").technology=="large_vehicle"]
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
        ds.add_par('demand',IWT_dat)
        ds.add_par('demand',road_dem)
        ds.add_par('demand',rail_dat)
        ds.add_par('bound_activity_up',road_dat)
    if freight_IWT:
        IWTF_dat=ds.par("demand")[ds.par("demand").commodity=="f_transport_IWT"]
        roadF_dat=ds.par("demand")[ds.par("demand").commodity=="f_transport_road"]
        railF_dat=ds.par("demand")[ds.par("demand").commodity=="f_transport_rail"]
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
        ds.add_par('demand',IWTF_dat)
        ds.add_par('demand',roadF_dat)
        ds.add_par('demand',railF_dat)

    # if True:
    #     pumps_dat=ds.par("bound_activity_lo")[ds.par("bound_activity_lo").technology=="oil_agri-pump"]
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
                ds.add_par("bound_activity_lo",par)
    # #             ds.add_par("bound_activity_up",par)

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
                ds.add_par("bound_activity_lo",par)
    #             ds.add_par("bound_activity_up",par)

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
                    ds.add_par("bound_activity_lo",par)
                    ds.add_par("bound_activity_up",par)
                else:
                    ds.add_par("bound_activity_lo",par)

    comment = 'WB_test_scenario'
    ds.commit(comment)
    ds.set_as_default()
    ds.solve(model='MESSAGE')
    #Model name is model_nm and scenario name is comment
    #Check
    reporting(mp, ds, 'False', model_nm, comment, merge_hist=True)
    return(ds)
	return render_template('scenario.html')


if __name__ == "__main__":
	app.run() #app.run(port=5002) #app.run(host='0.0.0.0', port=80)
