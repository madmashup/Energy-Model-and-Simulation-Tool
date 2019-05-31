import ixmp
import os
import sys
# Retrieve MESSAGE_DATA_PATH
msg_data_path = os.environ['MESSAGE_DATA_PATH']
postprocess_path = '{}/post-processing/'.format(msg_data_path)
if postprocess_path not in sys.path:
    sys.path.append(postprocess_path)
import pp_utils
import postprocess
from ixmp import default_paths
import pandas as pd
import os
import argparse

#---------------------------Cin-File------------------------------------------------------------------------------#
# Function for extraction technologies


def retr_extraction(model, scenario, pp, pp_utils):
    df = None
    vars = {}
    units = 'GWa'

    Coal = pp.extr('coal', units)
    Lignite = pp_utils._make_zero()
    Conv_oil = pp.extr(['oil'], units)
    Unconv_oil = pp_utils._make_zero()
    Conv_gas = pp.extr(['gas'], units)
    Unconv_gas = pp_utils._make_zero()
    nuc_domestic = pp.out(['ur_extr'], units, outfilter={
                          'commodity': ['nuclear']})
    nuc_import = pp.out(['ur_import'], units, outfilter={
                        'commodity': ['nuclear']})
    Uranium = (pp.out(['nuc_ppl'], units, outfilter={'commodity': [
        'electricity']}) * (nuc_domestic / (nuc_domestic + nuc_import)).fillna(0) / .35).fillna(0)

    vars['Coal'] = Coal + Lignite
    vars['Gas|Conventional'] = Conv_gas
    vars['Gas|Unconventional'] = Unconv_gas
    vars['Oil|Conventional'] = Conv_oil
    vars['Oil|Unconventional'] = Unconv_oil
    vars['Uranium'] = Uranium

    df = pp_utils.make_outputdf(vars, units, model, scenario, glb=False)
    return(df)

# Function for extraction technologies


def retr_cumulative_extraction(model, scenario, pp, pp_utils):
    df = None
    # Output Variables
    vars = {}
    units = 'TWa'

    Coal = pp_utils.cum_vals(pp.extrc('coal', units))
    Lignite = pp_utils._make_zero()
    Conv_oil = pp_utils.cum_vals(pp.extrc(['oil'], units))
    Unconv_oil = pp_utils._make_zero()
    Conv_gas = pp_utils.cum_vals(pp.extrc(['gas'], units))
    Unconv_gas = pp_utils._make_zero()
    Uranium = pp_utils._make_zero()

    vars['Coal'] = Coal + Lignite
    vars['Gas|Conventional'] = Conv_gas
    vars['Gas|Unconventional'] = Unconv_gas
    vars['Oil|Conventional'] = Conv_oil
    vars['Oil|Unconventional'] = Unconv_oil
    vars['Uranium'] = Uranium

    df = pp_utils.make_outputdf(vars, units, model, scenario, glb=False)
    return(df)

# Function for extraction technologies


def retr_remaining_resources(model, scenario, pp, pp_utils):
    df = None
    # Output Variables
    vars = {}
    units = 'TWa'

    Coal = pp_utils._make_zero()
    Lignite = pp_utils._make_zero()
    Conv_oil = pp_utils._make_zero()
    Unconv_oil = pp_utils._make_zero()
    Conv_gas = pp_utils._make_zero()
    Unconv_gas = pp_utils._make_zero()
    Uranium = pp_utils._make_zero()

    vars['Coal'] = Coal + Lignite
    vars['Gas|Conventional'] = Conv_gas
    vars['Gas|Unconventional'] = Unconv_gas
    vars['Oil|Conventional'] = Conv_oil
    vars['Oil|Unconventional'] = Unconv_oil
    vars['Uranium'] = Uranium

    df = pp_utils.make_outputdf(vars, units, model, scenario, glb=False)
    pp_utils.cum_vals(df)
    return(df)


def retr_pop(model, scenario, pp, pp_utils):
    units = 'million'
    vars = {}
    vars['Total'] = pp.act('Population')
    # SSP2 Shares are currently hardcoded
    col = [1990, 1995, 2000, 2005, 2010, 2020, 2030,
           2040, 2050, 2060, 2070, 2080, 2090, 2100]
    idx = pd.MultiIndex.from_product([['AFR', 'CPA', 'EEU', 'FSU', 'LAM', 'MEA', 'NAM', 'PAO', 'PAS', 'SAS', 'WEU', 'World']],
                                     names=['Region'])
    df_urban_perc = pd.DataFrame([[28.377, 30.576, 32.599, 34.802, 37.111, 42.498, 47.539, 52.225, 56.526, 60.42, 63.927, 67.057, 69.84, 72.312],
                                  [26.894, 31.077, 35.657, 41.963, 46.178, 53.615, 59.72,
                                   64.64, 68.603, 71.828, 74.481, 76.69, 78.547, 80.122],
                                  [59.286, 59.794, 59.836, 60.486, 61.408, 66.59, 70.722,
                                   74.045, 76.762, 79.007, 80.879, 82.46, 83.809, 84.966],
                                  [65.41, 64.869, 64.427, 64.086, 64.256, 68.243, 71.79,
                                   74.973, 77.811, 80.325, 82.498, 84.368, 85.968, 87.326],
                                  [70.351, 72.981, 75.383, 77.638, 79.524, 82.552, 84.956,
                                   86.893, 88.474, 89.778, 90.856, 91.757, 92.521, 93.173],
                                  [52.081, 54.147, 55.949, 57.763, 59.829, 64.712, 68.767,
                                   72.214, 75.138, 77.613, 79.705, 81.483, 83.017, 84.35],
                                  [75.393, 77.421, 79.321, 80.908, 82.303, 85.088, 87.497,
                                   89.566, 91.329, 92.818, 94.07, 95.117, 95.987, 96.709],
                                  [66.28, 67.814, 68.575, 69.518, 70.624, 74.587, 78.128,
                                   81.241, 83.964, 86.317, 88.336, 90.054, 91.509, 92.727],
                                  [38.271, 41.394, 44.906, 46.197, 47.806, 53.525, 58.636,
                                   63.099, 66.938, 70.221, 73.029, 75.429, 77.485, 79.254],
                                  [25.037, 26.144, 27.322, 28.491, 29.911, 35.669, 41.499,
                                   47.195, 52.606, 57.621, 62.185, 66.284, 69.927, 73.143],
                                  [71.14, 71.975, 72.872, 74.275, 75.691, 78.947, 81.712,
                                   84.072, 86.112, 87.885, 89.421, 90.749, 91.89, 92.868],
                                  [42.616, 44.45, 46.398, 48.627, 50.525, 58.453, 65.621, 71.912, 77.29, 81.78, 85.453, 88.414, 90.769, 92.627]], idx, col) / 100
    df_urban_perc = df_urban_perc.reset_index().set_index('Region')
    vars['Urban'] = pp.act('Population').multiply(
        df_urban_perc, fill_value=0)
    vars['Rural'] = pp.act('Population') - vars['Urban']
    df = pp_utils.make_outputdf(vars, units, model, scenario, glb=False)
    return(df)


def retr_demands_input(model, scenario, pp, pp_utils):
    units = 'GWa'
    vars = {}
    vars['Input|Agriculture Pumping'] = pp.inp(
        ['elec_agri-pump', 'elec-og_agri-pump', 'oil_agri-pump'], units)
    vars['Input|Commercial Hotwater'] = pp.inp(
        ['elec_comm-HW', 'thermal-og_comm-HW'], units)
    vars['Input|Commercial Other'] = pp.inp(
        ['elec_comm-oth', 'elec-og_comm-oth'], units)
    vars['Input|Residential Cooking'] = pp.inp(
        ['bio_cooking', 'coal_cooking', 'elec_cooking', 'gas_cooking', 'oil_cooking', 'traditional chullah'], units)
    vars['Input|Industry Specific'] = pp.inp('elec_ind-specific', units)
    vars['Input|Industry Thermal'] = pp.inp(
        ['coal_ind-thermal', 'elec_ind-thermal', 'gas_ind-thermal', 'oil_ind-thermal'], units)
    vars['Input|Residential Hotwater'] = pp.inp(
        ['elec_res-HW', 'thermal-og_res-HW'], units)
    vars['Input|Residential Other'] = pp.inp(
        ['elec_res-oth', 'elec-og_res-oth', 'oil_res-oth'], units)
    vars['Input|Transport Agriculture'] = pp.inp('oil_agri-trp', units)
    vars['Input|Transport Passenger Air'] = pp.inp('oil_AIRP', units)
    vars['Input|Transport Passenger Rail'] = pp.inp(
        ['elec_RAILP', 'oil_RAILP'], units)
    vars['Input|Transport Passenger Road'] = pp.inp(
        ['large_vehicle', 'small_vehicle'], units)
    vars['Input|Transport Freight Air'] = pp.inp('oil_AIRF', units)
    vars['Input|Transport Freight Rail'] = pp.inp(
        ['elec_RAILF', 'oil_RAILF'], units)
    vars['Input|Transport Freight Road'] = pp.inp(
        ['oil_HDVF_road', 'oil_LDVF_road'], units)
    vars['Input|Transport Freight IWT'] = pp.inp(
        ['oil_HDVF_IWT'], units)
    vars['Input|Transport Passenger IWT'] = pp.inp(
        ['oil_LargeP_IWT'], units)
    df = pp_utils.make_outputdf(vars, units, model, scenario, glb=False)

    return(df)


def retr_demands_output(model, scenario, pp, pp_utils):
    dfs = []

    units = 'GWa'
    vars = {}
    vars['Agriculture Pumping'] = pp.dem('agri_pumping', units)
    vars['Commercial Hotwater'] = pp.dem('commercial_hotwater', units)
    vars['Commercial Other'] = pp.dem('commercial_other', units)
    vars['Industry Specific'] = pp.dem('industry_specific', units)
    vars['Industry Thermal'] = pp.dem('industry_thermal', units)
    vars['Residential Cooking'] = pp.dem('cooking', units)
    vars['Residential Hotwater'] = pp.dem('residential_hotwater', units)
    vars['Residential Other'] = pp.dem('residential_other', units)
    dfs.append(pp_utils.make_outputdf(vars, units, model, scenario, glb=False))

    units = 'tractors'
    vars = {}
    vars['Transport Agriculture'] = pp.dem('agri_transport', units)
    dfs.append(pp_utils.make_outputdf(vars, units, model, scenario, glb=False))

    units = 'bpkm'
    vars = {}
    vars['Transport Passenger Air'] = pp.dem('p_transport_air', units)
    vars['Transport Passenger Rail'] = pp.dem('p_transport_rail', units)
    vars['Transport Passenger Road'] = pp.dem('p_transport_road', units)
    vars['Transport Passenger IWT'] = pp.dem('p_transport_IWT', units)
    dfs.append(pp_utils.make_outputdf(vars, units, model, scenario, glb=False))

    units = 'btkm'
    vars = {}
    vars['Transport Freight Air'] = pp.dem('f_transport_air', units)
    vars['Transport Freight Rail'] = pp.dem('f_transport_rail', units)
    vars['Transport Freight Road'] = pp.dem('f_transport_road', units)
    vars['Transport Freight IWT'] = pp.dem('f_transport_IWT', units)
    dfs.append(pp_utils.make_outputdf(vars, units, model, scenario, glb=False))

    df = pd.concat(dfs)
    return(df)


def retr_price(model, scenario, pp, pp_utils):
    # Carbon Price - GLB is set to MAX
    units = 'US$2010/tCO2'
    dfs = []
    vars = {}
    vars['Carbon'] = pp.cprc(units) / 10.
    dfs.append(pp_utils.make_outputdf(
        vars, units, model, scenario, glb=False))

    # Energy Prices - GLB is set to MAX
    #units = 'US$2010/GJ'
    units = 'US$2010/GWa'

    # Carbon price must also be multiplied by factor as retrieved prices are
    # already converted to $/GJ
    #_cbprc = pp.cprc('US$2010/tC') / 10. * 0.03171
    _cbprc = pp.cprc('US$2010/MtCO2') / 10.
    vars = {}
    # Final Energy prices are reported including the carbon price
    vars['Final Energy|Electricity'] = pp.eneprc(
        enefilter={'commodity': ['electricity'], 'level': ['final']}, units=units)
    vars['Final Energy|Gases|Natural Gas'] = pp.eneprc(
        enefilter={'commodity': ['gas'], 'level': ['final']}, units=units)
    vars['Final Energy|Gases|Hydrogen'] = pp.eneprc(
        enefilter={'commodity': ['hydrogen'], 'level': ['final']}, units=units)
    vars['Final Energy|Liquids|Biomass'] = pp_utils._make_zero()
    vars['Final Energy|Liquids|Oil'] = pp.eneprc(
        enefilter={'commodity': ['oil'], 'level': ['final']}, units=units)
    vars['Final Energy|Solids|Biomass'] = pp.eneprc(
        enefilter={'commodity': ['biomass'], 'level': ['final']}, units=units)
    vars['Final Energy|Solids|Coal'] = pp.eneprc(
        enefilter={'commodity': ['coal'], 'level': ['final']}, units=units)

    # Final Energy prices are reported excluding the carbon price
    vars['Final Energy wo carbon price|Electricity'] = pp.eneprc(
        enefilter={'commodity': ['electricity'], 'level': ['final']}, units=units)
    vars['Final Energy wo carbon price|Gases|Natural Gas'] = pp.eneprc(
        enefilter={'commodity': ['gas'], 'level': ['final']}, units=units) - _cbprc * 0.482
    vars['Final Energy wo carbon price|Liquids|Biomass'] = pp_utils._make_zero()
    vars['Final Energy wo carbon price|Liquids|Oil'] = pp.eneprc(
        enefilter={'commodity': ['oil'], 'level': ['final']}, units=units) - _cbprc * 0.631
    vars['Final Energy wo carbon price|Solids|Biomass'] = pp.eneprc(
        enefilter={'commodity': ['biomass'], 'level': ['final']}, units=units)
    vars['Final Energy wo carbon price|Solids|Coal'] = pp.eneprc(
        enefilter={'commodity': ['coal'], 'level': ['final']}, units=units) - _cbprc * 0.814

    # Primary energy prices are reported excluding the carbon price, hence the
    # subtraction of the carbonprice * the carbon content of the fuel
    vars['Primary Energy|Coal'] = pp.eneprc(enefilter={'commodity': ['coal'], 'level': [
                                            'primary']}, units=units) - _cbprc * 0.814
    vars['Primary Energy|Gas'] = pp.eneprc(enefilter={'commodity': ['gas'], 'level': [
                                           'primary']}, units=units) - _cbprc * 0.482
    vars['Primary Energy|Oil'] = pp.eneprc(enefilter={'commodity': ['oil'], 'level': [
                                           'primary']}, units=units) - _cbprc * 0.631
    vars['Primary Energy|Biomass'] = pp.eneprc(
        enefilter={'commodity': ['biomass'], 'level': ['primary']}, units=units)

    # Primary energy prices are reported including the carbon price
    vars['Primary Energy w carbon price|Coal'] = pp.eneprc(
        enefilter={'commodity': ['coal'], 'level': ['primary']}, units=units)
    vars['Primary Energy w carbon price|Gas'] = pp.eneprc(
        enefilter={'commodity': ['gas'], 'level': ['primary']}, units=units)
    vars['Primary Energy w carbon price|Oil'] = pp.eneprc(
        enefilter={'commodity': ['oil'], 'level': ['primary']}, units=units)
    vars['Primary Energy w carbon price|Biomass'] = pp.eneprc(
        enefilter={'commodity': ['biomass'], 'level': ['primary']}, units=units)

    # Secondary energy prices are reported excluding the carbon price, hence the subtraction of the carbonprice * the carbon content of the fuel
    vars['Secondary Energy|Electricity'] = pp.eneprc(
        enefilter={'commodity': ['electricity'], 'level': ['secondary']}, units=units)
    vars['Secondary Energy|Gases|Natural Gas'] = pp.eneprc(
        enefilter={'commodity': ['gas'], 'level': ['secondary']}, units=units) - _cbprc * 0.482
    vars['Secondary Energy|Liquids'] = pp_utils._make_zero()
    vars['Secondary Energy|Liquids|Biomass'] = pp_utils._make_zero()
    vars['Secondary Energy|Liquids|Oil'] = pp.eneprc(
        enefilter={'commodity': ['oil'], 'level': ['secondary']}, units=units) - _cbprc * 0.631
    vars['Secondary Energy|Solids|Biomass'] = pp.eneprc(
        enefilter={'commodity': ['biomass'], 'level': ['secondary']}, units=units)
    vars['Secondary Energy|Solids|Coal'] = pp.eneprc(enefilter={'commodity': [
                                                     'coal'], 'level': ['secondary']}, units=units) - _cbprc * 0.814

    # Secondary energy prices are reported including the carbon price
    vars['Secondary Energy w carbon price|Hydrogen'] = pp.eneprc(
        enefilter={'commodity': ['hydrogen'], 'level': ['secondary']}, units=units)
    vars['Secondary Energy w carbon price|Liquids'] = pp_utils._make_zero()
    vars['Secondary Energy w carbon price|Liquids|Biomass'] = pp_utils._make_zero()
    vars['Secondary Energy w carbon price|Liquids|Oil'] = pp.eneprc(
        enefilter={'commodity': ['oil'], 'level': ['secondary']}, units=units)

    dfs.append(pp_utils.make_outputdf(vars, units, model, scenario, glb=False))

    return(pd.concat(dfs))


def retr_othemi(var, model, scenario, pp, pp_utils):
    df = None
    # Output Variables
    vars = {}

    if var in ['BC', 'OC']:
        emia = '%sA' % var
    elif var == 'Sulfur':
        emia = 'SO2'
    else:
        emia = var

    # set units
    if emia == 'N2O':
        units = 'kt %s/yr' % var
    elif emia == 'SO2':
        units = 'Mt %s/yr' % emia
    else:
        units = 'Mt %s/yr' % var

    # Agriculture (Table 1)
    # Agriculture Waste Bruning
    if emia in ['BCA', 'OCA', 'SO2', 'NH3']:
        AgricultureWasteBurning = pp.emi('%s_AgWasteEM' % emia, 'GWa', emifilter={'relation': [
            '%s_Emission' % emia]}, emission_units=units)  # HACK: Variable activity is in kt so GWa -> MWa will * by .001
    elif emia in ['CO', 'NOx', 'VOC']:
        AgricultureWasteBurning = pp.emi('%s_AgWasteEM' % emia, 'GWa', emifilter={'relation': [
            '%s_nonenergy' % emia]}, emission_units=units)
    elif emia in ['CH4']:
        AgricultureWasteBurning = pp.emi('%s_AgWasteEM' % emia, 'GWa', emifilter={'relation': [
            '%s_new_Emission' % emia]}, emission_units=units)
    else:
        AgricultureWasteBurning = pp_utils._make_zero()
    vars['AFOLU|Biomass Burning'] = AgricultureWasteBurning
    # Other Agriculture
    if emia in 'CH4':
        vars['AFOLU|Agriculture|Livestock|Enteric Fermentation'] = pp.land_out(lu_out_filter={'level': [
                                                                               'land_use_reporting'], 'commodity': ['Emissions|CH4|Land Use|Agriculture|Enteric Fermentation']})
        vars['AFOLU|Agriculture|Livestock|Manure Management'] = pp.land_out(lu_out_filter={
                                                                            'level': ['land_use_reporting'], 'commodity': ['Emissions|CH4|Land Use|Agriculture|AWM']})
        vars['AFOLU|Agriculture|Livestock'] = vars['AFOLU|Agriculture|Livestock|Enteric Fermentation'] + \
            vars['AFOLU|Agriculture|Livestock|Manure Management']
        vars['AFOLU|Agriculture|Rice'] = pp.land_out(lu_out_filter={'level': [
                                                     'land_use_reporting'], 'commodity': ['Emissions|CH4|Land Use|Agriculture|Rice']})
        Agriculture = vars['AFOLU|Agriculture|Livestock|Enteric Fermentation'] + vars[
            'AFOLU|Agriculture|Livestock|Manure Management'] + vars['AFOLU|Agriculture|Rice']
        # Argriculture	= pp_utils._make_zero()
    elif emia in 'NH3':
        vars['AFOLU|Agriculture|Livestock|Manure Management'] = pp.land_out(
            lu_out_filter={'level': ['land_use_reporting'], 'commodity': ['NH3_ManureEM']}, units=units)
        vars['AFOLU|Agriculture|Livestock'] = vars[
            'AFOLU|Agriculture|Livestock|Manure Management']
        vars['AFOLU|Agriculture|Rice'] = pp.land_out(lu_out_filter={
                                                     'level': ['land_use_reporting'], 'commodity': ['NH3_RiceEM']}, units=units)
        vars['AFOLU|Agriculture|Managed Soils'] = pp.land_out(lu_out_filter={
                                                              'level': ['land_use_reporting'], 'commodity': ['NH3_SoilEM']}, units=units)
        Agriculture = vars['AFOLU|Agriculture|Livestock|Manure Management'] + \
            vars['AFOLU|Agriculture|Rice'] + \
            vars['AFOLU|Agriculture|Managed Soils']
    elif emia in 'NOx':
        vars['AFOLU|Agriculture|Managed Soils'] = pp.land_out(lu_out_filter={
                                                              'level': ['land_use_reporting'], 'commodity': ['NOx_SoilEM']}, units=units)
        Agriculture = vars['AFOLU|Agriculture|Managed Soils']
    elif emia in 'N2O':
        vars['AFOLU|Land|Grassland Pastures'] = pp.land_out(lu_out_filter={'level': [
            'land_use_reporting'], 'commodity': ['Emissions|N2O|Land Use|Agriculture|Pasture']})
        vars['AFOLU|Agriculture|Managed Soils'] = pp.land_out(lu_out_filter={'level': [
                                                              'land_use_reporting'], 'commodity': ['Emissions|N2O|Land Use|Agriculture|Cropland Soils']})
        vars['AFOLU|Agriculture|Livestock|Manure Management'] = pp.land_out(lu_out_filter={
                                                                            'level': ['land_use_reporting'], 'commodity': ['Emissions|N2O|Land Use|Agriculture|AWM']})
        vars['AFOLU|Agriculture|Livestock'] = vars[
            'AFOLU|Agriculture|Livestock|Manure Management']
        Agriculture = vars['AFOLU|Agriculture|Managed Soils'] + \
            vars['AFOLU|Agriculture|Livestock|Manure Management']
        # Argriculture	= pp_utils._make_zero()
    else:
        Agriculture = pp_utils._make_zero()
    vars['AFOLU|Agriculture'] = Agriculture

    # Grassland Burning (Table 2)
    if emia in ['BCA', 'OCA', 'SO2', 'NH3', 'CO', 'NOx', 'VOC', 'CH4']:
        GrasslandBurning = pp.land_out(lu_out_filter={'level': [
                                       'land_use_reporting'], 'commodity': ['%s_SavanBurnEM' % emia]}, units=units)
    else:
        GrasslandBurning = pp_utils._make_zero()
    vars['AFOLU|Land|Grassland Burning'] = GrasslandBurning

    # Forest Burning (Table 3)
    if emia in ['BCA', 'OCA', 'SO2', 'NH3', 'CO', 'NOx', 'VOC', 'CH4']:
        ForestBurning = pp.land_out(lu_out_filter={'level': [
                                    'land_use_reporting'], 'commodity': ['%s_LandUseChangeEM' % emia]}, units=units)
    else:
        ForestBurning = pp_utils._make_zero()
    vars['AFOLU|Land|Forest Burning'] = ForestBurning

    # Aircraft (Table 4)
    Aircraft = pp.emi('aviation_Emission', 'GWa', emifilter={
                      'relation': ['%s_Emission' % emia]}, emission_units=units)
    vars['Energy|Demand|Transportation|Aviation|International'] = Aircraft

    # Electricity and heat production (Table 5)
    Heat = pp.emi(['bio_hpl', 'coal_hpl', 'foil_hpl', 'gas_hpl'], 'GWa', emifilter={
                  'relation': ['%s_Emission' % emia]}, emission_units=units)
    vars['Energy|Supply|Heat'] = Heat

    # Comment OFR: In next variable - Added all technologies as of second row
    # (all CCS technologies were forgotten)
    Powergeneration = pp.emi(['bio_istig', 'bio_ppl', 'coal_adv', 'coal_ppl_u', 'igcc', 'coal_ppl', 'foil_ppl', 'gas_cc', 'gas_ppl', 'loil_cc', 'loil_ppl', 'oil_ppl', 'SO2_scrub_ppl',
                              'coal_adv_ccs', 'igcc_ccs', 'gas_cc_ccs', 'gas_ct', 'gas_htfc', 'bio_istig_ccs'], 'GWa', emifilter={'relation': ['%s_Emission' % emia]}, emission_units=units)
    vars['Energy|Supply|Electricity'] = Powergeneration

    # Fuel Production and Transformation AND Other Fugitive/Flaring (Table 6)
    ExtractionSolids = pp.emi(['coal_extr_ch4', 'coal_extr', 'lignite_extr'], 'GWa', emifilter={
                              'relation': ['%s_Emission' % emia]}, emission_units=units)
    Distribution_Solids = pp.emi('coal_t_d', 'GWa', emifilter={'relation': [
                                 '%s_Emission' % emia]}, emission_units=units)
    vars['Energy|Supply|Solids and Other Fugitive'] = Distribution_Solids + \
        ExtractionSolids

    # Comment OFR: In next variable - Added all technologies as of third row
    # (h2_bio tecs were forgotten) despite these not having emissions - SET
    Synfuels = pp.emi(['eth_bio', 'eth_bio_ccs', 'liq_bio', 'liq_bio_ccs', 'meth_coal', 'meth_coal_ccs', 'meth_ng', 'meth_ng_ccs', 'coal_gas', 'h2_smr', 'h2_smr_ccs',
                       'h2_coal', 'h2_coal_ccs', 'syn_liq', 'syn_liq_ccs', 'SO2_scrub_synf',
                       'h2_bio', 'h2_bio_ccs'], 'GWa', emifilter={'relation': ['%s_Emission' % emia]}, emission_units=units)
    Extraction = pp.emi('%s_RemainingEM' % emia, 'GWa', emifilter={'relation': ['%s_new_Emission' % emia]}, emission_units=units) \
        + pp.emi(['gas_extr_1', 'gas_extr_2', 'gas_extr_3', 'gas_extr_4', 'gas_extr_5', 'gas_extr_6', 'gas_extr_7', 'gas_extr_8',
                  'oil_extr_1_ch4', 'oil_extr_1', 'oil_extr_2_ch4', 'oil_extr_2', 'oil_extr_3_ch4', 'oil_extr_3', 'oil_extr_4_ch4', 'oil_extr_4', 'oil_extr_5', 'oil_extr_6', 'oil_extr_7', 'oil_extr_8',
                  'flaring_CO2'], 'GWa', emifilter={'relation': ['%s_Emission' % emia]}, emission_units=units)
    Distribution = pp.emi(['gas_t_d_ch4', 'gas_t_d', 'loil_t_d', 'foil_t_d', 'heat_t_d', 'biomass_t_d', 'meth_t_d', 'eth_t_d', 'h2_t_d', 'lh2_t_d', 'elec_t_d'], 'GWa',
                          emifilter={'relation': ['%s_Emission' % emia]}, emission_units=units)
    Refineries = pp.emi(['ref_lol', 'ref_hil', 'SO2_scrub_ref'], 'GWa', emifilter={
                        'relation': ['%s_Emission' % emia]}, emission_units=units)
    vars['Energy|Supply|Fuel Production and Transformation'] = Synfuels + \
        Extraction + Distribution + Refineries

    # Industrial Combustion (Table 7)
    SpecificInd = pp.emi(['sp_coal_I', 'sp_el_I', 'sp_liq_I', 'sp_meth_I', 'sp_eth_I', 'solar_pv_I',
                          'h2_fc_I', 'sp_h2_I'], 'GWa', emifilter={'relation': ['%s_Emission' % emia]}, emission_units=units)
    ThermalInd = pp.emi(['coal_i', 'foil_i', 'loil_i', 'gas_i', 'meth_i', 'eth_i', 'h2_i', 'biomass_i', 'elec_i', 'heat_i', 'hp_el_i', 'hp_gas_i', 'solar_i'], 'GWa',
                        emifilter={'relation': ['%s_Emission' % emia]}, emission_units=units)
    addvarInd = pp_utils._make_zero()
    if emia == 'SO2':
        # SO2_Scrubber // SO2_IndNonEnergyEM // SO2_coal_t/d
        addvarInd = addvarInd + pp.emi(['SO2_scrub_ind', 'coal_t_d-in-SO2', 'coal_t_d-in-06p'],
                                       'GWa', emifilter={'relation': ['%s_Emission' % emia]}, emission_units=units)

    IndustrialCombustion = SpecificInd + ThermalInd + addvarInd
    vars['Energy|Demand|Industry'] = IndustrialCombustion

    # Industrial process and product use (Table 8)
    if emia in ['BCA', 'OCA', 'SO2', 'NH3']:
        NonEnergyInd = pp.emi('%s_IndNonEnergyEM' % emia, 'GWa', emifilter={
                              'relation': ['%s_Emission' % emia]}, emission_units=units)
    elif emia in ['CO', 'NOx', 'VOC']:
        NonEnergyInd = pp.emi('%s_IndNonEnergyEM' % emia, 'GWa', emifilter={
                              'relation': ['%s_nonenergy' % emia]}, emission_units=units)
    elif emia in ['CH4']:
        NonEnergyInd = pp.emi('%s_IndNonEnergyEM' % emia, 'GWa', emifilter={
                              'relation': ['%s_new_Emission' % emia]}, emission_units=units)
    else:
        NonEnergyInd = pp_utils._make_zero()
    addvarNEIND = pp_utils._make_zero()
    if emia == 'N2O':
        addvarNEIND = addvarNEIND + pp.emi(['N2On_nitric', 'N2On_adipic', 'adipic_thermal', 'nitric_catalytic1', 'nitric_catalytic2',
                                            'nitric_catalytic3', 'nitric_catalytic4', 'nitric_catalytic5', 'nitric_catalytic6'], 'GWa', emifilter={'relation': ['%s_nonenergy' % emia]}, emission_units=units)
    vars['Industrial Processes'] = NonEnergyInd + addvarNEIND

    # International shipping (Table 9)
    if emia == 'NH3':
        Bunker = pp_utils._make_zero()
    else:
        Bunker = pp.emi(['foil_bunker', 'loil_bunker', 'eth_bunker', 'meth_bunker', 'LNG_bunker', 'LH2_bunker'],
                        'GWa', emifilter={'relation': ['%s_Emission_bunkers' % emia]}, emission_units=units)
    vars['Energy|Demand|Transportation|Shipping|International'] = Bunker

    vars['Energy|Supply|Gases and Liquids Fugitive'] = pp_utils._make_zero()

    # Residential, Commercial, Other - Other (Table 11)
    if emia == 'N2O':
        ResComOth = pp.emi('%s_OtherAgSo' % emia, 'GWa', emifilter={
                           'relation': ['%sother' % emia]}, emission_units=units) + \
            pp.emi('%s_ONonAgSo' % emia, 'GWa', emifilter={
                'relation': ['%sother' % emia]}, emission_units=units)
    else:
        ResComOth = pp_utils._make_zero()
    vars['Energy|Demand|AFOFI'] = ResComOth

    # Residential, Commercial, Other - Residential, Commercial (Table 12)
    ResComSpec = pp.emi(['sp_el_RC', 'solar_pv_RC', 'h2_fc_RC'], 'GWa', emifilter={
                        'relation': ['%s_Emission' % emia]}, emission_units=units)
    ResComTherm = pp.emi(['coal_rc', 'foil_rc', 'loil_rc', 'gas_rc', 'elec_rc', 'biomass_rc', 'heat_rc', 'meth_rc', 'eth_rc', 'h2_rc', 'hp_el_rc', 'hp_gas_rc', 'solar_rc'],
                         'GWa', emifilter={'relation': ['%s_Emission' % emia]}, emission_units=units)
    ResComNC = pp.emi('biomass_nc', 'GWa', emifilter={'relation': [
                      '%s_Emission' % emia]}, emission_units=units)
    OtherSC = pp.emi('other_sc', 'GWa', emifilter={'relation': [
                     '%s_Emission' % emia]}, emission_units=units)
    addvarRC = pp_utils._make_zero()
    if emia == 'SO2':
        addvarRC = addvarRC + pp.emi(['coal_t_d-rc-SO2', 'coal_t_d-rc-06p'], 'GWa', emifilter={
                                     'relation': ['%s_Emission' % emia]}, emission_units=units)
    vars['Energy|Demand|Residential and Commercial'] = ResComSpec + \
        ResComTherm + ResComNC + OtherSC + addvarRC

    # Road transportation (Table 13)
    Transport = pp.emi(['coal_trp', 'foil_trp', 'loil_trp', 'gas_trp', 'elec_trp', 'meth_ic_trp', 'eth_ic_trp', 'meth_fc_trp', 'eth_fc_trp', 'h2_fc_trp'],
                       'GWa', emifilter={'relation': ['%s_Emission' % emia]}, emission_units=units)
    # vars['Energy|Demand|Transportation|Road'] = Transport
    vars['Energy|Demand|Transportation|Road Rail and Domestic Shipping'] = Transport

    # Solvents production and application (Table 14)
    if emia == 'VOC':
        Solvents = pp.emi('%s_SolventEM' % emia, 'GWa', emifilter={
                          'relation': ['%s_nonenergy' % emia]}, emission_units=units)
    else:
        Solvents = pp_utils._make_zero()
    vars['Product Use|Solvents'] = Solvents

    # Waste (Table 15)
    if emia == 'N2O':
        Waste = pp.emi('%s_Human' % emia, 'GWa', emifilter={
                       'relation': ['%sother' % emia]}, emission_units=units)
    elif emia == 'CH4':
        Waste = pp.emi(['CH4_WasteBurnEM', 'CH4_DomWasteWa', 'CH4_IndWasteWa'], 'GWa', emifilter={'relation': ['CH4_new_Emission']}, emission_units=units) \
            + pp.emi(['CH4n_landfills', 'landfill_digester1', 'landfill_compost1', 'landfill_mechbio', 'landfill_heatprdn', 'landfill_direct1', 'landfill_ele', 'landfill_flaring'],
                     'GWa', emifilter={'relation': ['CH4_nonenergy']}, emission_units=units)
    elif emia in ['VOC', 'CO', 'NOx']:
        Waste = pp.emi('%s_WasteBurnEM' % emia, 'GWa', emifilter={
                       'relation': ['%s_nonenergy' % emia]}, emission_units=units)
    elif emia in ['BCA', 'OCA', 'SO2']:
        Waste = pp.emi('%s_WasteBurnEM' % emia, 'GWa', emifilter={
                       'relation': ['%s_Emission' % emia]}, emission_units=units)
    else:
        Waste = pp_utils._make_zero()
    vars['Waste'] = Waste

    df = pp_utils.make_outputdf(vars, units, model, scenario, glb=False)
    return(df)


def retr_CO2_CCS(model, scenario, pp, pp_utils):
    vars = {}
    ene_units = 'GWa'
    emi_units = 'Mt CO2/GWa'
    output_units = 'Mt CO2/yr'
    emifilter = {'emission': ['CO2']}

#    # Biogas share calculation
#    _Biogas = pp.out('gas_bio', 'EJ/yr')
#    # Example of a set
#    _gas_inp_tecs = ['gas_ppl', 'gas_cc', 'gas_cc_ccs', 'gas_ct', 'gas_htfc', 'gas_hpl',
#                     'meth_ng', 'meth_ng_ccs', 'h2_smr', 'h2_smr_ccs', 'gas_t_d', 'gas_t_d_ch4']
#    _totgas = pp.inp(_gas_inp_tecs, 'EJ/yr', inpfilter={'commodity': ['gas']})
#    _BGas_share = (_Biogas / _totgas).fillna(0)

    _CCS_coal_elec = -1. * pp.act_emif(['coal_usc_ccs', 'igcc_ccs'],
                                       emiffilter=emifilter, ene_units=ene_units, emi_units=emi_units)
    _CCS_coal_liq = -1. * pp_utils._make_zero()
    _CCS_coal_hydrogen = -1. * pp_utils._make_zero()
    _CCS_cement = -1. * pp_utils._make_zero()
    _CCS_bio_elec = -1. * pp_utils._make_zero()
    _CCS_bio_liq = -1. * pp_utils._make_zero()
    _CCS_bio_hydrogen = -1. * pp_utils._make_zero()
    _CCS_gas_elec = -1. * \
        pp.act_emif(['gas_cc_ccs_ppl'], emiffilter=emifilter,
                    ene_units=ene_units, emi_units=emi_units)
    _CCS_gas_liq = -1. * pp_utils._make_zero()
    _CCS_gas_hydrogen = -1. * pp_utils._make_zero()

    vars['CCS|Fossil|Energy|Supply|Electricity'] = _CCS_coal_elec + _CCS_gas_elec
    vars['CCS|Fossil|Energy|Supply|Liquids'] = _CCS_coal_liq + _CCS_gas_liq
    vars['CCS|Fossil|Energy|Supply|Hydrogen'] = _CCS_coal_hydrogen + \
        _CCS_gas_hydrogen
    vars['CCS|Biomass|Energy|Supply|Electricity'] = _CCS_bio_elec + _CCS_gas_elec
    vars['CCS|Biomass|Energy|Supply|Liquids'] = _CCS_bio_liq + _CCS_gas_liq
    vars['CCS|Biomass|Energy|Supply|Hydrogen'] = _CCS_bio_hydrogen + \
        _CCS_gas_hydrogen
    vars['CCS|Industrial Processes'] = _CCS_cement
    vars['Land Use'] = pp_utils._make_zero()
    vars['Land Use|Afforestation'] = pp_utils._make_zero()

    df = pp_utils.make_outputdf(vars, output_units, model, scenario, glb=False)
    return(df)

# Function - Cin-File for CO2 emissions
# Note, that when the activity of a powerplant is retrieved, the units are
# set to 'GWa', the same as the model units so that the conversion factor
# is 1.


def retr_CO2emi(model, scenario, pp, pp_utils):
    # Electricity - > Elec
    # Transmission -> Trnmi
    # Transportation -> Trn
    # Passenger -> Pass
    # Transformation -> Tranfrm
    # Residential -> Res
    # Commercial -> Com
    # Fuel Produciton and Transformation -> FuelTandP

    vars = {}
    ene_units = 'GWa'
    emi_units = 'Mt CO2/GWa'
    output_units = 'Mt CO2/yr'
    emifilter = {'emission': ['CO2']}

    #### AFOLU ####
    vars['AFOLU'] = pp_utils._make_zero()

    #### Energy Demand ####
    # AFOFI
    vars['Energy|Demand|AFOFI|Pumping|Oil'] = pp.act_emif(
        ['oil_agri-pump'], emiffilter=emifilter, ene_units=ene_units, emi_units=emi_units)
    vars['Energy|Demand|AFOFI|Pumping'] = vars['Energy|Demand|AFOFI|Pumping|Oil']
    vars['Energy|Demand|AFOFI|Transport|Oil'] = pp.act_emif(
        ['oil_agri-trp'], emiffilter=emifilter, ene_units=ene_units, emi_units=emi_units)
    vars['Energy|Demand|AFOFI|Transport'] = vars['Energy|Demand|AFOFI|Transport|Oil']
    vars['Energy|Demand|AFOFI'] = vars['Energy|Demand|AFOFI|Transport'] + \
        vars['Energy|Demand|AFOFI|Pumping']

    # Commercial
    vars['Energy|Demand|Com'] = pp_utils._make_zero()

    # Residential
    vars['Energy|Demand|Res|Cooking|Oil'] = pp.act_emif(
        ['oil_cooking'], emiffilter=emifilter, ene_units=ene_units, emi_units=emi_units)
    vars['Energy|Demand|Res|Cooking|Gas'] = pp.act_emif(
        ['gas_cooking'], emiffilter=emifilter, ene_units=ene_units, emi_units=emi_units)
    vars['Energy|Demand|Res|Cooking|Coal'] = pp.act_emif(
        ['coal_cooking'], emiffilter=emifilter, ene_units=ene_units, emi_units=emi_units)
    vars['Energy|Demand|Res|Cooking'] = vars['Energy|Demand|Res|Cooking|Oil'] + \
        vars['Energy|Demand|Res|Cooking|Gas'] + \
        vars['Energy|Demand|Res|Cooking|Coal']
    vars['Energy|Demand|Res|Other|Oil'] = pp.act_emif(
        ['oil_res-oth'], emiffilter=emifilter, ene_units=ene_units, emi_units=emi_units)
    vars['Energy|Demand|Res|Other'] = vars['Energy|Demand|Res|Other|Oil']
    vars['Energy|Demand|Res'] = vars['Energy|Demand|Res|Cooking'] + \
        vars['Energy|Demand|Res|Other']

    # Residential and Commercial (and AFOFI)
    vars['Energy|Demand|Res and Com'] = vars['Energy|Demand|Com'] + \
        vars['Energy|Demand|Res']
    vars['Energy|Demand|Res and Com and AFOFI'] = vars['Energy|Demand|Com'] + \
        vars['Energy|Demand|Res'] + vars['Energy|Demand|AFOFI']

    # Industry
    vars['Energy|Demand|Industry|Oil'] = pp.act_emif(
        ['oil_ind-thermal'], emiffilter=emifilter, ene_units=ene_units, emi_units=emi_units)
    vars['Energy|Demand|Industry|Gas'] = pp.act_emif(
        ['gas_ind-thermal'], emiffilter=emifilter, ene_units=ene_units, emi_units=emi_units)
    vars['Energy|Demand|Industry|Coal'] = pp.act_emif(
        ['coal_ind-thermal'], emiffilter=emifilter, ene_units=ene_units, emi_units=emi_units)
    vars['Energy|Demand|Industry'] = vars['Energy|Demand|Industry|Coal'] + \
        vars['Energy|Demand|Industry|Gas'] + \
        vars['Energy|Demand|Industry|Oil']

    # Transportation
    # Aviation
    vars['Energy|Demand|Trn|Aviation|Domestic|Freight|Oil'] = pp.act_emif(
        ['oil_AIRF'], emiffilter=emifilter, ene_units=ene_units, emi_units=emi_units)
    vars['Energy|Demand|Trn|Aviation|Domestic|Freight'] = vars['Energy|Demand|Trn|Aviation|Domestic|Freight|Oil']
    vars['Energy|Demand|Trn|Aviation|Domestic|Pass|Oil'] = pp.act_emif(
        ['oil_AIRP'], emiffilter=emifilter, ene_units=ene_units, emi_units=emi_units)
    vars['Energy|Demand|Trn|Aviation|Domestic|Pass'] = vars['Energy|Demand|Trn|Aviation|Domestic|Pass|Oil']
    vars['Energy|Demand|Trn|Aviation|Domestic'] = vars['Energy|Demand|Trn|Aviation|Domestic|Pass'] + \
        vars['Energy|Demand|Trn|Aviation|Domestic|Freight']
    vars['Energy|Demand|Trn|Aviation|International'] = pp_utils._make_zero()
    vars['Energy|Demand|Trn|Aviation'] = vars['Energy|Demand|Trn|Aviation|International'] + \
        vars['Energy|Demand|Trn|Aviation|Domestic']
    # Rail
    vars['Energy|Demand|Trn|Rail|Freight|Oil'] = pp.act_emif(
        ['oil_RAILF'], emiffilter=emifilter, ene_units=ene_units, emi_units=emi_units)
    vars['Energy|Demand|Trn|Rail|Freight'] = vars['Energy|Demand|Trn|Rail|Freight|Oil']
    vars['Energy|Demand|Trn|Rail|Pass|Oil'] = pp.act_emif(
        ['oil_RAILP'], emiffilter=emifilter, ene_units=ene_units, emi_units=emi_units)
    vars['Energy|Demand|Trn|Rail|Pass'] = vars['Energy|Demand|Trn|Rail|Pass|Oil']
    vars['Energy|Demand|Trn|Rail'] = vars['Energy|Demand|Trn|Rail|Pass'] + \
        vars['Energy|Demand|Trn|Rail|Freight']
    # Road
    vars['Energy|Demand|Trn|Road|Freight|Oil|LDV'] = pp.act_emif(
        ['oil_LDVF_road'], emiffilter=emifilter, ene_units=ene_units, emi_units=emi_units)
    vars['Energy|Demand|Trn|Road|Freight|Oil|HDV'] = pp.act_emif(
        ['oil_HDVF_road'], emiffilter=emifilter, ene_units=ene_units, emi_units=emi_units)
    vars['Energy|Demand|Trn|Road|Freight|Oil'] = vars['Energy|Demand|Trn|Road|Freight|Oil|HDV'] + \
        vars['Energy|Demand|Trn|Road|Freight|Oil|LDV']
    vars['Energy|Demand|Trn|Road|Freight'] = vars['Energy|Demand|Trn|Road|Freight|Oil']
    vars['Energy|Demand|Trn|Road|Pass|Gas|Small'] = pp.act_emif(
        ['gas_SmallP_road'], emiffilter=emifilter, ene_units=ene_units, emi_units=emi_units)
    vars['Energy|Demand|Trn|Road|Pass|Gas|Large'] = pp.act_emif(
        ['gas_LargeP_road'], emiffilter=emifilter, ene_units=ene_units, emi_units=emi_units)
    vars['Energy|Demand|Trn|Road|Pass|Gas'] = vars['Energy|Demand|Trn|Road|Pass|Gas|Large'] + \
        vars['Energy|Demand|Trn|Road|Pass|Gas|Small']
    vars['Energy|Demand|Trn|Road|Pass|Oil|Small'] = pp.act_emif(
        ['oil_SmallP_road'], emiffilter=emifilter, ene_units=ene_units, emi_units=emi_units)
    vars['Energy|Demand|Trn|Road|Pass|Oil|Large'] = pp.act_emif(
        ['oil_LargeP_road'], emiffilter=emifilter, ene_units=ene_units, emi_units=emi_units)
    vars['Energy|Demand|Trn|Road|Pass|Oil'] = vars['Energy|Demand|Trn|Road|Pass|Oil|Large'] + \
        vars['Energy|Demand|Trn|Road|Pass|Oil|Small']
    vars['Energy|Demand|Trn|Road|Pass'] = vars['Energy|Demand|Trn|Road|Pass|Oil'] + \
        vars['Energy|Demand|Trn|Road|Pass|Gas']
    vars['Energy|Demand|Trn|Road'] = vars['Energy|Demand|Trn|Road|Pass'] + \
        vars['Energy|Demand|Trn|Road|Freight']
    # Shipping
    vars['Energy|Demand|Trn|Shipping'] = pp_utils._make_zero()
    vars['Energy|Demand|Trn|IWT|Pass|Oil'] = pp.act_emif(
        ['oil_LargeP_IWT'], emiffilter=emifilter, ene_units=ene_units, emi_units=emi_units)
    vars['Energy|Demand|Trn|IWT|Freight|Oil'] = pp.act_emif(
        ['oil_HDVF_IWT'], emiffilter=emifilter, ene_units=ene_units, emi_units=emi_units)
    vars['Energy|Demand|Trn|IWT|Freight'] = vars['Energy|Demand|Trn|IWT|Freight|Oil']
    vars['Energy|Demand|Trn|IWT|Pass'] = vars['Energy|Demand|Trn|IWT|Pass|Oil']
    vars['Energy|Demand|Trn|IWT'] = vars['Energy|Demand|Trn|IWT|Pass'] + \
        vars['Energy|Demand|Trn|IWT|Freight']
    # Trn Aggregates
#     vars['Energy|Demand|Trn|Rail and Domestic Shipping'] = vars['Energy|Demand|Trn|Rail'] + \
#         vars['Energy|Demand|Trn|Shipping']
#     vars['Energy|Demand|Trn|Road Rail and Domestic Shipping'] = vars['Energy|Demand|Trn|Road'] + \
#         vars['Energy|Demand|Trn|Rail'] + \
#         vars['Energy|Demand|Trn|Shipping'] + \
#         vars['Energy|Demand|Trn|IWT']
    
    #### Energy Supply ####
    # Electricity and heat production
    vars['Energy|Supply|Heat'] = pp_utils._make_zero()
    vars['Energy|Supply|Elec|Oil|Diesel Generator'] = pp.act_emif(
        ['dg_set'], emiffilter=emifilter, ene_units=ene_units, emi_units=emi_units)
    vars['Energy|Supply|Elec|Oil'] = vars['Energy|Supply|Elec|Oil|Diesel Generator']

    vars['Energy|Supply|Elec|Gas|w/ CCS|gas_cc_ccs_ppl'] = pp.act_emif(
        ['gas_cc_ccs_ppl'], emiffilter=emifilter, ene_units=ene_units, emi_units=emi_units)
    vars['Energy|Supply|Elec|Gas|w/ CCS'] = vars['Energy|Supply|Elec|Gas|w/ CCS|gas_cc_ccs_ppl']
    vars['Energy|Supply|Elec|Gas|w/o CCS|gas_cc_peak_ppl'] = pp.act_emif(
        ['gas_cc_peak_ppl'], emiffilter=emifilter, ene_units=ene_units, emi_units=emi_units)
    vars['Energy|Supply|Elec|Gas|w/o CCS|gas_cc_ppl'] = pp.act_emif(
        ['gas_cc_ppl'], emiffilter=emifilter, ene_units=ene_units, emi_units=emi_units)
    vars['Energy|Supply|Elec|Gas|w/o CCS'] = vars['Energy|Supply|Elec|Gas|w/o CCS|gas_cc_peak_ppl'] + \
        vars['Energy|Supply|Elec|Gas|w/o CCS|gas_cc_ppl']
    vars['Energy|Supply|Elec|Gas'] = vars['Energy|Supply|Elec|Gas|w/o CCS'] + \
        vars['Energy|Supply|Elec|Gas|w/ CCS']

    vars['Energy|Supply|Elec|Coal|w/ CCS|igcc_ccs'] = pp.act_emif(
        ['igcc_ccs'], emiffilter=emifilter, ene_units=ene_units, emi_units=emi_units)
    vars['Energy|Supply|Elec|Coal|w/ CCS|coal_usc_ccs'] = pp.act_emif(
        ['coal_usc_ccs'], emiffilter=emifilter, ene_units=ene_units, emi_units=emi_units)
    vars['Energy|Supply|Elec|Coal|w/ CCS'] = vars['Energy|Supply|Elec|Coal|w/ CCS|igcc_ccs'] + \
        vars['Energy|Supply|Elec|Coal|w/ CCS|coal_usc_ccs']
    vars['Energy|Supply|Elec|Coal|w/o CCS|igcc'] = pp.act_emif(
        ['igcc'], emiffilter=emifilter, ene_units=ene_units, emi_units=emi_units)
    vars['Energy|Supply|Elec|Coal|w/o CCS|coal_usc'] = pp.act_emif(
        ['coal_usc'], emiffilter=emifilter, ene_units=ene_units, emi_units=emi_units)
    vars['Energy|Supply|Elec|Coal|w/o CCS|coal_ppl'] = pp.act_emif(
        ['coal_ppl'], emiffilter=emifilter, ene_units=ene_units, emi_units=emi_units)
    vars['Energy|Supply|Elec|Coal|w/o CCS|coal_ppl_sub'] = pp.act_emif(
        ['coal_ppl_sub'], emiffilter=emifilter, ene_units=ene_units, emi_units=emi_units)
    vars['Energy|Supply|Elec|Coal'] = vars['Energy|Supply|Elec|Coal|w/o CCS|igcc'] + vars['Energy|Supply|Elec|Coal|w/o CCS|coal_usc'] + \
        vars['Energy|Supply|Elec|Coal|w/o CCS|coal_ppl'] + \
        vars['Energy|Supply|Elec|Coal|w/o CCS|coal_ppl_sub']
    vars['Energy|Supply|Elec'] = vars['Energy|Supply|Elec|Coal'] + \
        vars['Energy|Supply|Elec|Gas'] + \
        vars['Energy|Supply|Elec|Oil']

    # Fuel Production and Transformation AND Other Fugitive/Flaring
    vars['Energy|Supply|Solids and Other Fugitive'] = pp.act_emif(
        ['coal_t/d'], emiffilter=emifilter, ene_units=ene_units, emi_units=emi_units)
    vars['Energy|Supply|FuelPandT|Tranfrm|Oil'] = pp.act_emif(
        ['oil_refinery'], emiffilter=emifilter, ene_units=ene_units, emi_units=emi_units)
    vars['Energy|Supply|FuelPandT|Tranfrm|Gas|LNG Import'] = pp.act_emif(
        ['gas_import_terminal'], emiffilter=emifilter, ene_units=ene_units, emi_units=emi_units)
    vars['Energy|Supply|FuelPandT|Tranfrm|Gas|Hydrogen'] = pp.act_emif(
        ['h2_smr'], emiffilter=emifilter, ene_units=ene_units, emi_units=emi_units)
    vars['Energy|Supply|FuelPandT|Tranfrm|Gas'] = vars['Energy|Supply|FuelPandT|Tranfrm|Gas|Hydrogen'] + \
        vars['Energy|Supply|FuelPandT|Tranfrm|Gas|LNG Import']
    vars['Energy|Supply|FuelPandT|Tranfrm'] = vars['Energy|Supply|FuelPandT|Tranfrm|Gas'] + \
        vars['Energy|Supply|FuelPandT|Tranfrm|Oil']
    vars['Energy|Supply|FuelPandT|Trnmi|Gas'] = pp.act_emif(
        ['gas_t/d'], emiffilter=emifilter, ene_units=ene_units, emi_units=emi_units)
    vars['Energy|Supply|FuelPandT|Trnmi|Oil'] = pp.act_emif(
        ['oil_t/d'], emiffilter=emifilter, ene_units=ene_units, emi_units=emi_units)
    vars['Energy|Supply|FuelPandT|Trnmi'] = vars['Energy|Supply|FuelPandT|Trnmi|Oil'] + \
        vars['Energy|Supply|FuelPandT|Trnmi|Gas']
    vars['Energy|Supply|FuelPandT'] = vars['Energy|Supply|FuelPandT|Trnmi'] + \
        vars['Energy|Supply|FuelPandT|Tranfrm']

    # Industrial Combustion
#     vars['Energy|Demand|Industry'] = pp_utils._make_zero()

    # Industrial process and product use (Table 8)
    vars['Industrial Processes'] = pp_utils._make_zero()

    # Oil and Gas Fugitive/Flaring
    vars['Energy|Supply|Gases and Liquids Fugitive'] = pp_utils._make_zero()

    # Solvents production and application
    vars['Product Use|Solvents'] = pp_utils._make_zero()

    # Waste
    vars['Waste'] = pp_utils._make_zero()

    df = pp_utils.make_outputdf(vars, output_units, model, scenario, glb=False)
    return(df)

# Function - Cin-File for centralized heat generation


def retr_SE_district_heat(model, scenario, pp, pp_utils):
    vars = {}
    units = 'EJ/yr'

    vars['Biomass'] = pp.out('bio_hpl', units)
    vars['Geothermal'] = pp.out('geo_hpl', units)
    vars['Coal'] = pp.out('coal_hpl', units)
    vars['Oil'] = pp.out('foil_hpl', units)
    vars['Gas'] = pp.out('gas_hpl', units)
    Passout_turbine = pp.out('po_turbine', units)
    vars['Other'] = Passout_turbine

    df = pp_utils.make_outputdf(vars, units, model, scenario, glb=False)
    return(df)

# Function - Cin-File for synthetic fuels


def retr_SE_synfuels(model, scenario, pp, pp_utils):
    vars = {}
    units = 'EJ/yr'

    vars['Liquids|Oil'] = \
        pp.out('ref_lol', units, outfilter={'level': ['secondary'], 'commodity': ['lightoil']}) \
        + pp.out('ref_lol', units, outfilter={'level': ['secondary'], 'commodity': ['fueloil']}) \
        + pp.out('ref_hil', units, outfilter={'level': ['secondary'], 'commodity': ['lightoil']}) \
        + pp.out('ref_hil', units,
                 outfilter={'level': ['secondary'], 'commodity': ['fueloil']})
    vars['Liquids|Biomass|w/o CCS'] = pp.out(['eth_bio', 'liq_bio'], units, outfilter={
                                             'level': ['primary'], 'commodity': ['ethanol']})
    vars['Liquids|Biomass|w/ CCS'] = pp.out(['eth_bio_ccs', 'liq_bio_ccs'], units, outfilter={
                                            'level': ['primary'], 'commodity': ['ethanol']})
    vars['Liquids|Coal|w/o CCS'] = pp.out(['meth_coal', 'syn_liq'], units, outfilter={
                                          'level': ['primary'], 'commodity': ['methanol']})
    vars[
        'Liquids|Coal|w/ CCS'] = pp.out(['meth_coal_ccs', 'syn_liq_ccs'], units, outfilter={'level': ['primary'], 'commodity': ['methanol']})
    vars['Liquids|Gas|w/o CCS'] = pp.out('meth_ng', units, outfilter={
                                         'level': ['primary'], 'commodity': ['methanol']})
    vars['Liquids|Gas|w/ CCS'] = pp.out('meth_ng_ccs', units, outfilter={
                                        'level': ['primary'], 'commodity': ['methanol']})
    vars['Hydrogen|Coal|w/o CCS'] = pp.out('h2_coal', units, outfilter={
                                           'level': ['secondary'], 'commodity': ['hydrogen']})
    vars['Hydrogen|Coal|w/ CCS'] = pp.out('h2_coal_ccs', units, outfilter={
                                          'level': ['secondary'], 'commodity': ['hydrogen']})
    vars['Hydrogen|Gas|w/o CCS'] = pp.out('h2_smr', units, outfilter={
                                          'level': ['secondary'], 'commodity': ['hydrogen']})
    vars['Hydrogen|Gas|w/ CCS'] = pp.out('h2_smr_ccs', units, outfilter={
                                         'level': ['secondary'], 'commodity': ['hydrogen']})
    vars['Hydrogen|Biomass|w/o CCS'] = pp.out(
        'h2_bio', units, outfilter={'level': ['secondary'], 'commodity': ['hydrogen']})
    vars['Hydrogen|Biomass|w/ CCS'] = pp.out('h2_bio_ccs', units, outfilter={
                                             'level': ['secondary'], 'commodity': ['hydrogen']})
    vars['Hydrogen|Electricity'] = pp.out('h2_elec', units, outfilter={
                                          'level': ['secondary'], 'commodity': ['hydrogen']})
#	vars['Hydrogen|Nuclear'] = H2_nuc = x5th:act * ,:eff
#	vars['Hydrogen|Solar'] = pp.out('x.sh', units)

    df = pp_utils.make_outputdf(vars, units, model, scenario, glb=False)
    return(df)


def retr_SE_gases(model, scenario, pp, pp_utils):
    vars = {}
    units = 'GWa'

    vars['Natural Gas'] = pp.out(['gas_bal'], units)
    vars['Coal'] = pp_utils._make_zero()
    vars['Biomass'] = pp_utils._make_zero()
    vars['Other'] = pp_utils._make_zero()

    df = pp_utils.make_outputdf(vars, units, model, scenario, glb=False)
    return(df)


def retr_SE_solids(model, scenario, pp, pp_utils):
    vars = {}
    units = 'GWa'

    BiomassNC = pp.inp('traditional chullah', units)
    BiomassCooking = pp.inp('bio_cooking', units)

    CoalIND = pp.inp(['coal_ind-thermal'], units)
    CoalCooking = pp.inp(['coal_cooking'], units)

    vars['Biomass'] = BiomassNC + BiomassCooking
    vars['Coal'] = CoalIND + CoalCooking

    df = pp_utils.make_outputdf(vars, units, model, scenario, glb=False)
    return(df)

# Function - Cin-File for electricity generation


def retr_SE_elecgen(model, scenario, pp, pp_utils):
    vars = {}
    units = 'GWa'

    # Elec production from Coal split into uses with and without CCS
    vars['Coal|w/ CCS|coal_usc_ccs'] = pp.out(['coal_usc_ccs'], units)
    vars['Coal|w/ CCS|igcc_ccs'] = pp.out(['igcc_ccs'], units)
    vars['Coal|w/ CCS'] = vars['Coal|w/ CCS|coal_usc_ccs'] + \
        vars['Coal|w/ CCS|igcc_ccs']
    vars['Coal|w/o CCS|coal_ppl_sub'] = pp.out(['coal_ppl_sub'], units)
    vars['Coal|w/o CCS|coal_ppl'] = pp.out(['coal_ppl'], units)
    vars['Coal|w/o CCS|coal_usc'] = pp.out(['coal_usc'], units)
    vars['Coal|w/o CCS|igcc'] = pp.out(['igcc'], units)
    vars['Coal|w/o CCS'] = vars['Coal|w/o CCS|coal_ppl_sub'] + \
        vars['Coal|w/o CCS|coal_ppl'] + \
        vars['Coal|w/o CCS|coal_usc'] + vars['Coal|w/o CCS|igcc']

    # Electricity production from Gas split into uses with and without CCS
    vars['Gas|w/ CCS|gas_cc_ccs_ppl'] = pp.out(['gas_cc_ccs_ppl'], units)
    vars['Gas|w/ CCS'] = vars['Gas|w/ CCS|gas_cc_ccs_ppl']
    vars['Gas|w/o CCS|gas_cc_ppl'] = pp.out(['gas_cc_ppl'], units)
    vars['Gas|w/o CCS|gas_cc_peak_ppl'] = pp.out(['gas_cc_peak_ppl'], units)
    vars['Gas|w/o CCS'] = vars['Gas|w/o CCS|gas_cc_ppl'] + \
        vars['Gas|w/o CCS|gas_cc_peak_ppl']

    # Electricity production from Oil split into uses with and without CCS
    vars['Oil|w CCS'] = pp_utils._make_zero()
    vars['Oil|w/o CCS'] = pp_utils._make_zero()

    # Electricity production from Biomass split into uses with and without CCS
    vars['Biomass|w/o CCS'] = pp.out(['bio_ppl'], units, outfilter={
                           'commodity': ['electricity']})
    vars['Biomass|w/ CCS'] = pp_utils._make_zero()

    # Electricity production from hydro
    vars['Hydro'] = pp.out(['hydro'], units, outfilter={
                           'commodity': ['electricity']})

    # Electricity generation from Solar split into PV and CSP
    vars['Solar|PV'] = pp.out(['solar_PV'], units)
    vars['Solar|CSP'] = pp.out(['solar_CSP'], units)

    # Electricity generation from Wind split into On- and Off-Shore
    vars['Wind|Offshore'] = pp.out(['wind_offshore'], units)
    vars['Wind|Onshore'] = pp.out(['wind_onshore'], units)

    # Electricity generation from Geothermal
    vars['Geothermal'] = pp_utils._make_zero()

    # Electricity generation from Nuclear
    vars['Nuclear'] = pp.out(['nuc_ppl'], units)

    # Electricity generation from other sources
    vars['Other'] = pp_utils._make_zero()

    # Electricity losses
    vars['Storage Losses'] = pp_utils._make_zero()
    vars['Transmission Losses'] = pp.out('elec_grid', units) - pp.inp('elec_grid', units)

    df = pp_utils.make_outputdf(vars, units, model, scenario, glb=False)
    return(df)


def retr_pe(model, scenario, pp, pp_utils):
    vars = {}

    # This table replicates Table: "Primary Energy Consumption (incl. CCS
    # shares), Direct Equivalent" from db_input.cin
    units = 'GWa'
    # :inp -> pp.inp()
    # :act -> pp.inp()
    # :out -> pp.out()

    groupby = ['Region']

    # Primary Energy Coal
    vars['Coal|Domestic'] = pp.inp(['coal_extr'], units, inpfilter={
                                   'commodity': ['coal', 'lignite']})
    vars['Coal|Import'] = pp.out(['coal_imp'], units, outfilter={
                                 'commodity': ['coal']})
    vars['Coal'] = vars['Coal|Domestic'] + vars['Coal|Import']

    # Primary Energy Coal split into uses with and without CCS
    vars['Coal|w/ CCS|coal_usc_ccs'] = pp.inp(
        ['coal_usc_ccs'], units, inpfilter={'commodity': ['coal']})
    vars['Coal|w/ CCS|igcc_ccs'] = pp.inp(['igcc_ccs'],
                                          units, inpfilter={'commodity': ['coal']})
    vars['Coal|w/ CCS'] = vars['Coal|w/ CCS|coal_usc_ccs'] + \
        vars['Coal|w/ CCS|igcc_ccs']
    vars['Coal|w/o CCS|coal_ppl_sub'] = pp.inp(
        ['coal_ppl_sub'], units, inpfilter={'commodity': ['coal']})
    vars['Coal|w/o CCS|coal_ppl'] = pp.inp(['coal_ppl'],
                                           units, inpfilter={'commodity': ['coal']})
    vars['Coal|w/o CCS|coal_usc'] = pp.inp(['coal_usc'],
                                           units, inpfilter={'commodity': ['coal']})
    vars['Coal|w/o CCS|igcc'] = pp.inp(['igcc'],
                                       units, inpfilter={'commodity': ['coal']})
    vars['Coal|w/o CCS'] = vars['Coal'] - vars['Coal|w/ CCS']

    # Primary Energy Gas
    vars['Gas|Domestic'] = pp.inp(
        ['gas_extr'], units, inpfilter={'commodity': ['gas']})
    vars['Gas|Import'] = pp.out(
        ['gas_imp'], units, outfilter={'commodity': ['gas']})
    vars['Gas'] = vars['Gas|Domestic'] + vars['Gas|Import']

    # Primary Energy Gas split into uses with and without CCS
    vars['Gas|w/ CCS|gas_cc_ccs_ppl'] = pp.inp(
        ['gas_cc_ccs_ppl'], units, inpfilter={'commodity': ['gas']})
    vars['Gas|w/ CCS'] = vars['Gas|w/ CCS|gas_cc_ccs_ppl']

    vars['Gas|w/o CCS|gas_cc_ppl'] = pp.inp(
        ['gas_cc_ppl'], units, inpfilter={'commodity': ['gas']})
    vars['Gas|w/o CCS|gas_cc_peak_ppl'] = pp.inp(
        ['gas_cc_peak_ppl'], units, inpfilter={'commodity': ['gas']})
    vars['Gas|w/o CCS'] = vars['Gas'] - vars['Gas|w/ CCS']

    # Primary Energy Oil
    vars['Oil|Domestic'] = pp.inp(
        ['oil_extr'], units, inpfilter={'commodity': ['oil']})
    vars['Oil|Import'] = pp.out(
        ['oil_imp'], units, outfilter={'commodity': ['oil']})
    vars['Oil'] = vars['Oil|Domestic'] + vars['Oil|Import']

    # Primary Energy Biomass
    vars['Biomass|Modern'] = pp.out(['bio_extr'], units, outfilter={
                                    'commodity': ['biomass']})
    vars['Biomass|Traditional'] = pp.out(['biomass_nc'], units, outfilter={
                                         'commodity': ['biomass_nc']})
    vars['Biomass'] = vars['Biomass|Modern'] + vars['Biomass|Traditional']

    # Primary Energy Nuclear
    nuc_domestic = pp.out(['ur_extr'], units, outfilter={
                          'commodity': ['nuclear']})
    nuc_import = pp.out(['ur_import'], units, outfilter={
                        'commodity': ['nuclear']})
    vars['Nuclear|Domestic'] = (pp.out(['nuc_ppl'], units, outfilter={'commodity': [
                                'electricity']}) * (nuc_domestic / (nuc_domestic + nuc_import)).fillna(0) / .35).fillna(0)
    vars['Nuclear|Import'] = (pp.out(['nuc_ppl'], units, outfilter={'commodity': [
                              'electricity']}) * (nuc_import / (nuc_domestic + nuc_import)).fillna(0) / .35).fillna(0)

    # Primary Energy Hydro
    vars['Hydro'] = pp.out(['hydro'], units, outfilter={
                           'commodity': ['electricity']})

    # Primary Energy Solar
    # where should HW production be included?
    vars['Solar|Thermal'] = pp.out(['solar_thermal-HW_offgrid'],
                              units, outfilter={'commodity': ['og_heat']})
    vars['Renewable RPO'] = pp.out(['solar_RPO','solar_RPO_offgrid'], units, outfilter={
                               'commodity': ['og_electricity','electricity']})
    vars['Solar|PV'] = pp.out(['solar_PV', 'solar_PV_offgrid'],
                              units, outfilter={'commodity': ['electricity', 'og_electricity']})#-vars['Renewable RPO']
    vars['Solar|CSP'] = pp.out(['solar_CSP'], units, outfilter={
                               'commodity': ['electricity']})
    
    vars['Solar'] = vars['Solar|PV'] + vars['Solar|CSP'] + vars['Solar|Thermal']
#     vars['Renewable RPO'] = pp.out(['solar_RPO'], units, outfilter={
#                                'commodity': ['solar_PV']})
    # Primary Energy Wind
    vars['Wind|Offshore'] = pp.out(['wind_offshore'], units, outfilter={
                                   'commodity': ['electricity']})
    vars['Wind|Onshore'] = pp.out(['wind_onshore'], units, outfilter={
                                  'commodity': ['electricity']})
    vars['Wind'] = vars['Wind|Offshore'] + vars['Wind|Onshore']

    df = pp_utils.make_outputdf(vars, units, model, scenario, glb=False)
    return(df)


def retr_ppl_capparameters(prmfunc, model, scenario, pp, pp_utils):
    vars = {}
    units = 'GW'

    vars['Electricity|Biomass|w/ CCS'] = pp_utils._make_zero()
    vars['Electricity|Biomass|w/o CCS'] = prmfunc('bio_ppl')
    vars['Electricity|Coal|w/ CCS|coal_usc_ccs'] = prmfunc('coal_usc_ccs')
    vars['Electricity|Coal|w/ CCS|igcc_ccs'] = prmfunc('igcc_ccs')
    vars['Electricity|Coal|w/o CCS|coal_ppl_sub'] = prmfunc('coal_ppl_sub')
    vars['Electricity|Coal|w/o CCS|coal_ppl'] = prmfunc('coal_ppl')
    vars['Electricity|Coal|w/o CCS|coal_usc'] = prmfunc('coal_usc')
    vars['Electricity|Coal|w/o CCS|igcc'] = prmfunc('igcc')
    vars['Electricity|Gas|w/ CCS|gas_cc_ccs_ppl'] = prmfunc('gas_cc_ccs_ppl')
    vars['Electricity|Gas|w/o CCS|gas_cc_ppl'] = prmfunc('gas_cc_ppl')
    vars['Electricity|Gas|w/o CCS|gas_cc_peak_ppl'] = prmfunc('gas_cc_peak_ppl')
    vars['Electricity|Geothermal'] = pp_utils._make_zero()
    vars['Electricity|Hydro'] = prmfunc('hydro')
    vars['Electricity|Nuclear'] = prmfunc('nuc_ppl')
    vars['Electricity|Oil|w/ CCS'] = pp_utils._make_zero()
    vars['Electricity|Oil|w/o CCS'] = pp_utils._make_zero()
    vars['Electricity|Solar|CSP'] = prmfunc('solar_CSP')
    vars['Electricity|Solar|PV'] = prmfunc('solar_PV')
    vars['Electricity|Solar|Thermal'] = prmfunc('solar_thermal-HW_offgrid')
    vars['Electricity|Wind|Offshore'] = prmfunc('wind_offshore')
    vars['Electricity|Wind|Onshore'] = prmfunc('wind_onshore')
    vars['Gases|Biomass|w/o CCS'] = pp_utils._make_zero()
    vars['Gases|Coal|w/o CCS'] = pp_utils._make_zero()
    vars['Hydrogen|Biomass|w/ CCS'] = pp_utils._make_zero()
    vars['Hydrogen|Biomass|w/o CCS'] = pp_utils._make_zero()
    vars['Hydrogen|Coal|w/ CCS'] = pp_utils._make_zero()
    vars['Hydrogen|Coal|w/o CCS'] = pp_utils._make_zero()
    vars['Hydrogen|Electricity'] = pp_utils._make_zero()
    vars['Hydrogen|Gas|w/ CCS'] = pp_utils._make_zero()
    vars['Hydrogen|Gas|w/o CCS'] = prmfunc('h2_smr')
    vars['Liquids|Biomass|w/ CCS'] = pp_utils._make_zero()
    vars['Liquids|Biomass|w/o CCS'] = pp_utils._make_zero()
    vars['Liquids|Coal|w/ CCS'] = pp_utils._make_zero()
    vars['Liquids|Coal|w/o CCS'] = pp_utils._make_zero()
    vars['Liquids|Gas|w/ CCS'] = pp_utils._make_zero()
    vars['Liquids|Gas|w/o CCS'] = pp_utils._make_zero()
    vars['Liquids|Oil|w/o CCS'] = prmfunc('oil_refinery')

    df = pp_utils.make_outputdf(vars, units, model, scenario, glb=False)
    return(df)


def retr_ppl_parameters(prmfunc, units, model, scenario, pp, pp_utils):
    vars = {}

    vars['Electricity|Biomass|w/ CCS'] = pp_utils._make_zero()
    vars['Electricity|Biomass|w/o CCS'] = prmfunc('bio_ppl', units=units)
    vars['Electricity|Coal|w/ CCS|coal_usc_ccs'] = prmfunc(
        'coal_usc_ccs', units=units)
    vars['Electricity|Coal|w/ CCS|igcc_ccs'] = prmfunc('igcc_ccs', units=units)
    vars['Electricity|Coal|w/o CCS|coal_ppl_sub'] = prmfunc(
        'coal_ppl_sub', units=units)
    vars['Electricity|Coal|w/o CCS|coal_ppl'] = prmfunc(
        'coal_ppl', units=units)
    vars['Electricity|Coal|w/o CCS|coal_usc'] = prmfunc(
        'coal_usc', units=units)
    vars['Electricity|Coal|w/o CCS|igcc'] = prmfunc('igcc', units=units)
    vars['Electricity|Gas|w/ CCS|gas_cc_ccs_ppl'] = prmfunc(
        'gas_cc_ccs_ppl', units=units)
    vars['Electricity|Gas|w/o CCS|gas_cc_ppl'] = prmfunc(
        'gas_cc_ppl', units=units)
    vars['Electricity|Gas|w/o CCS|gas_cc_peak_ppl'] = prmfunc(
        'gas_cc_peak_ppl', units=units)
    vars['Electricity|Geothermal'] = pp_utils._make_zero()
    vars['Electricity|Hydro'] = prmfunc('hydro', units=units)
    vars['Electricity|Nuclear'] = prmfunc('nuc_ppl', units=units)
    vars['Electricity|Oil|w/ CCS'] = pp_utils._make_zero()
    vars['Electricity|Oil|w/o CCS'] = pp_utils._make_zero()
    vars['Electricity|Solar|CSP'] = prmfunc('solar_CSP', units=units)
    vars['Electricity|Solar|PV'] = prmfunc('solar_PV', units=units)
    vars['Electricity|Solar|Thermal'] = prmfunc('solar_thermal-HW_offgrid', units=units)
    vars['Electricity|Wind|Offshore'] = prmfunc('wind_offshore', units=units)
    vars['Electricity|Wind|Onshore'] = prmfunc('wind_onshore', units=units)
    vars['Gases|Biomass|w/o CCS'] = pp_utils._make_zero()
    vars['Gases|Coal|w/o CCS'] = pp_utils._make_zero()
    vars['Hydrogen|Biomass|w/ CCS'] = pp_utils._make_zero()
    vars['Hydrogen|Biomass|w/o CCS'] = pp_utils._make_zero()
    vars['Hydrogen|Coal|w/ CCS'] = pp_utils._make_zero()
    vars['Hydrogen|Coal|w/o CCS'] = pp_utils._make_zero()
    vars['Hydrogen|Electricity'] = pp_utils._make_zero()
    vars['Hydrogen|Gas|w/ CCS'] = pp_utils._make_zero()
    vars['Hydrogen|Gas|w/o CCS'] = prmfunc('h2_smr', units=units)
    vars['Liquids|Biomass|w/ CCS'] = pp_utils._make_zero()
    vars['Liquids|Biomass|w/o CCS'] = pp_utils._make_zero()
    vars['Liquids|Coal|w/ CCS'] = pp_utils._make_zero()
    vars['Liquids|Coal|w/o CCS'] = pp_utils._make_zero()
    vars['Liquids|Gas|w/ CCS'] = pp_utils._make_zero()
    vars['Liquids|Gas|w/o CCS'] = pp_utils._make_zero()
    vars['Liquids|Oil|w/o CCS'] = prmfunc('oil_refinery', units=units)

    df = pp_utils.make_outputdf(vars, units, model, scenario, glb=False)
    return(df)


def retr_ppl_opcost_parameters(prmfunc, units, model, scenario, pp, pp_utils):
    vars = {}

    groupby = ['Region']
    formatting = 'reporting'

    vars['Electricity|Biomass|w/ CCS'] = pp_utils._make_zero()
    vars['Electricity|Biomass|w/o CCS'] = prmfunc(
        'bio_ppl', units=units, groupby=groupby, formatting=formatting)
    vars['Electricity|Coal|w/ CCS|coal_usc_ccs'] = prmfunc(
        'coal_usc_ccs', units=units, groupby=groupby, formatting=formatting)
    vars['Electricity|Coal|w/ CCS|igcc_ccs'] = prmfunc(
        'igcc_ccs', units=units, groupby=groupby, formatting=formatting)
    vars['Electricity|Coal|w/o CCS|coal_ppl_sub'] = prmfunc(
        'coal_ppl_sub', units=units, groupby=groupby, formatting=formatting)
    vars['Electricity|Coal|w/o CCS|coal_ppl'] = prmfunc(
        'coal_ppl', units=units, groupby=groupby, formatting=formatting)
    vars['Electricity|Coal|w/o CCS|coal_usc'] = prmfunc(
        'coal_usc', units=units, groupby=groupby, formatting=formatting)
    vars['Electricity|Coal|w/o CCS|igcc'] = prmfunc(
        'igcc', units=units, groupby=groupby, formatting=formatting)
    vars['Electricity|Gas|w/ CCS|gas_cc_ccs_ppl'] = prmfunc(
        'gas_cc_ccs_ppl', units=units, groupby=groupby, formatting=formatting)
    vars['Electricity|Gas|w/o CCS|gas_cc_ppl'] = prmfunc(
        'gas_cc_ppl', units=units, groupby=groupby, formatting=formatting)
    vars['Electricity|Gas|w/o CCS|gas_cc_peak_ppl'] = prmfunc(
        'gas_cc_peak_ppl', units=units, groupby=groupby, formatting=formatting)
    vars['Electricity|Geothermal'] = pp_utils._make_zero()
    vars['Electricity|Hydro'] = prmfunc(
        'hydro', units=units, groupby=groupby, formatting=formatting)
    vars['Electricity|Nuclear'] = prmfunc(
        'nuc_ppl', units=units, groupby=groupby, formatting=formatting)
    vars['Electricity|Oil|w/ CCS'] = pp_utils._make_zero()
    vars['Electricity|Oil|w/o CCS'] = pp_utils._make_zero()
    vars['Electricity|Solar|CSP'] = prmfunc(
        'solar_CSP', units=units, groupby=groupby, formatting=formatting)
    vars['Electricity|Solar|PV'] = prmfunc(
        'solar_PV', units=units, groupby=groupby, formatting=formatting)
    vars['Electricity|Wind|Offshore'] = prmfunc(
        'wind_offshore', units=units, groupby=groupby, formatting=formatting)
    vars['Electricity|Wind|Onshore'] = prmfunc(
        'wind_onshore', units=units, groupby=groupby, formatting=formatting)
    vars['Gases|Biomass|w/o CCS'] = pp_utils._make_zero()
    vars['Gases|Coal|w/o CCS'] = pp_utils._make_zero()
    vars['Hydrogen|Biomass|w/ CCS'] = pp_utils._make_zero()
    vars['Hydrogen|Biomass|w/o CCS'] = pp_utils._make_zero()
    vars['Hydrogen|Coal|w/ CCS'] = pp_utils._make_zero()
    vars['Hydrogen|Coal|w/o CCS'] = pp_utils._make_zero()
    vars['Hydrogen|Electricity'] = pp_utils._make_zero()
    vars['Hydrogen|Gas|w/ CCS'] = pp_utils._make_zero()
    vars['Hydrogen|Gas|w/o CCS'] = prmfunc('h2_smr', units=units, groupby=groupby, formatting=formatting)
    vars['Liquids|Biomass|w/ CCS'] = pp_utils._make_zero()
    vars['Liquids|Biomass|w/o CCS'] = pp_utils._make_zero()
    vars['Liquids|Coal|w/ CCS'] = pp_utils._make_zero()
    vars['Liquids|Coal|w/o CCS'] = pp_utils._make_zero()
    vars['Liquids|Gas|w/ CCS'] = pp_utils._make_zero()
    vars['Liquids|Gas|w/o CCS'] = pp_utils._make_zero()
    vars['Liquids|Oil|w/o CCS'] = prmfunc('oil_refinery', units=units, groupby=groupby, formatting=formatting)

    df = pp_utils.make_outputdf(vars, units, model, scenario, glb=False)
    return(df)


def retr_eff_parameters(model, scenario, pp, pp_utils):
    vars = {}
    units = '%'
    formatting = 'reporting'

    outfilter = {'level': ['secondary'], 'commodity': ['electricity']}

    vars['Electricity|Biomass|w/ CCS'] = pp_utils._make_zero()
    vars['Electricity|Biomass|w/o CCS'] = pp.eff('bio_ppl', inpfilter={
                                                 'commodity': ['biomass']}, outfilter=outfilter, formatting=formatting)
    vars['Electricity|Coal|w/ CCS|coal_usc_ccs'] = pp.eff('coal_usc_ccs', inpfilter={
                                                          'commodity': ['coal']}, outfilter=outfilter, formatting=formatting)
    vars['Electricity|Coal|w/ CCS|igcc_ccs'] = pp.eff('igcc_ccs', inpfilter={
                                                      'commodity': ['coal']}, outfilter=outfilter, formatting=formatting)
    vars['Electricity|Coal|w/o CCS|coal_ppl_sub'] = pp.eff('coal_ppl_sub', inpfilter={
                                                           'commodity': ['coal']}, outfilter=outfilter, formatting=formatting)
    vars['Electricity|Coal|w/o CCS|coal_ppl'] = pp.eff('coal_ppl', inpfilter={
                                                       'commodity': ['coal']}, outfilter=outfilter, formatting=formatting)
    vars['Electricity|Coal|w/o CCS|coal_usc'] = pp.eff('coal_usc', inpfilter={
                                                       'commodity': ['coal']}, outfilter=outfilter, formatting=formatting)
    vars['Electricity|Coal|w/o CCS|igcc'] = pp.eff('igcc', inpfilter={'commodity': [
                                                   'coal']}, outfilter=outfilter, formatting=formatting)
    vars['Electricity|Gas|w/ CCS|gas_cc_ccs_ppl'] = pp.eff('gas_cc_ccs_ppl', inpfilter={
                                                           'commodity': ['gas']}, outfilter=outfilter, formatting=formatting)
    vars['Electricity|Gas|w/o CCS|gas_cc_ppl'] = pp.eff('gas_cc_ppl', inpfilter={
                                                        'commodity': ['gas']}, outfilter=outfilter, formatting=formatting)
    vars['Electricity|Gas|w/o CCS|gas_cc_peak_ppl'] = pp.eff(
        'gas_cc_peak_ppl', inpfilter={'commodity': ['gas']}, outfilter=outfilter, formatting=formatting)
    vars['Electricity|Geothermal'] = pp_utils._make_zero()
    vars['Electricity|Hydro'] = pp_utils._make_zero()
    vars['Electricity|Nuclear'] = pp_utils._make_zero()
    vars['Electricity|Oil|w/ CCS'] = pp_utils._make_zero()
    vars['Electricity|Oil|w/o CCS'] = pp_utils._make_zero()
    vars['Electricity|Solar|CSP'] = pp_utils._make_zero()
    vars['Electricity|Solar|PV'] = pp_utils._make_zero()
    vars['Electricity|Solar|Thermal'] = pp_utils._make_zero()
    vars['Electricity|Wind|Offshore'] = pp_utils._make_zero()
    vars['Electricity|Wind|Onshore'] = pp_utils._make_zero()

    outfilter = {'level': [''], 'commodity': ['']}

    vars['Gases|Biomass|w/o CCS'] = pp_utils._make_zero()
    vars['Gases|Coal|w/o CCS'] = pp_utils._make_zero()

    outfilter = {'level': ['secondary'], 'commodity': ['hydrogen']}

    vars['Hydrogen|Biomass|w/ CCS'] = pp_utils._make_zero()
    vars['Hydrogen|Biomass|w/o CCS'] = pp_utils._make_zero()
    vars['Hydrogen|Coal|w/ CCS'] = pp_utils._make_zero()
    vars['Hydrogen|Coal|w/o CCS'] = pp_utils._make_zero()
    vars['Hydrogen|Electricity'] = pp_utils._make_zero()
    vars['Hydrogen|Gas|w/ CCS'] = pp_utils._make_zero()
    vars['Hydrogen|Gas|w/o CCS'] = pp.eff('h2_smr', inpfilter={'commodity': [
                                          'gas']}, outfilter=outfilter, formatting=formatting)

    outfilter = {'level': [''], 'commodity': ['']}

    vars['Liquids|Biomass|w/ CCS'] = pp_utils._make_zero()
    vars['Liquids|Biomass|w/o CCS'] = pp_utils._make_zero()

    outfilter = {'level': [''], 'commodity': ['']}

    vars['Liquids|Coal|w/ CCS'] = pp_utils._make_zero()
    vars['Liquids|Coal|w/o CCS'] = pp_utils._make_zero()

    outfilter = {'level': [''], 'commodity': ['']}

    vars['Liquids|Gas|w/ CCS'] = pp_utils._make_zero()
    vars['Liquids|Gas|w/o CCS'] = pp_utils._make_zero()

    outfilter = {'level': ['secondary'], 'commodity': ['oil']}

    vars['Liquids|Oil|w/o CCS'] = pp.eff('oil_refinery', inpfilter={'commodity': [
                                         'oil']}, outfilter=outfilter, formatting=formatting)

    # As the unit is '%', values retrieved from the database must be mutplied by 100.
    # For all years where the technologies are not included in the model, the fficiency is set to 0 (this includes global numbers)
    tmp = {}
    for v in vars.keys():
        tmp[v] = round(vars[v] * 100, 2)
    vars = tmp

    df = pp_utils.make_outputdf(vars, units, model, scenario, glb=False)
    return(df)


# Function - Cin-File for Efficiency


def retr_fe(model, scenario, pp, pp_utils):
    vars = {}

    # This table replicates Table: "Final Energy Consumption by sector and
    # fuel" from db_input.cin
    units = 'GWa'
    # :inp -> inp()
    # :act -> inp()
    # :out -> out()

    # Calculations needed:
    # 1. Share of offgrid electricity provided by diesel and pv
    og_pv_out = pp.out('solar_PV_offgrid', units)
    og_dg_out = pp.out('dg_set', units)
    _share_og_pv = (og_pv_out / (og_pv_out + og_dg_out)).fillna(0)
    _share_og_dg = (og_dg_out / (og_pv_out + og_dg_out)).fillna(0)

    # Accounting for the industry THERMAL sector
    BiomassINDTherm = pp_utils._make_zero()
    OilINDTherm = pp.inp('oil_ind-thermal', units)
    MethINDTherm = pp_utils._make_zero()
    EthINDTherm = pp_utils._make_zero()
    GasINDTherm = pp.inp('gas_ind-thermal', units)
    CoalINDTherm = pp.inp('coal_ind-thermal', units)
    ElecINDTherm = pp.inp(['elec_ind-thermal'], units)
    OnsitePVINDTherm = pp_utils._make_zero()
    DheatINDTherm = pp_utils._make_zero()
    SolThermINDTherm = pp_utils._make_zero()
    H2INDTherm = pp_utils._make_zero()

    vars['Industry Thermal|Electricity|Grid'] = ElecINDTherm
    vars['Industry Thermal|Electricity|Solar'] = OnsitePVINDTherm
    vars['Industry Thermal|Electricity'] = vars['Industry Thermal|Electricity|Grid'] + \
        vars['Industry Thermal|Electricity|Solar']
    vars['Industry Thermal|Gases'] = GasINDTherm
    vars['Industry Thermal|Heat'] = DheatINDTherm
    vars['Industry Thermal|Hydrogen'] = H2INDTherm
    vars['Industry Thermal|Liquids|Biomass'] = EthINDTherm
    vars['Industry Thermal|Liquids|Coal'] = MethINDTherm
    vars['Industry Thermal|Liquids|Gas'] = pp_utils._make_zero()
    vars['Industry Thermal|Liquids|Oil'] = OilINDTherm
    vars['Industry Thermal|Liquids'] = vars['Industry Thermal|Liquids|Biomass'] + \
        vars['Industry Thermal|Liquids|Coal'] + \
        vars['Industry Thermal|Liquids|Gas'] + \
        vars['Industry Thermal|Liquids|Oil']
    vars['Industry Thermal|Other'] = SolThermINDTherm
    vars['Industry Thermal|Solids|Coal'] = CoalINDTherm
    vars['Industry Thermal|Solids|Biomass'] = BiomassINDTherm
    vars['Industry Thermal|Solids'] = vars['Industry Thermal|Solids|Coal'] + \
        vars['Industry Thermal|Solids|Biomass']

    # Accounting for the industry SPECIFIC sector
    BiomassINDSpec = pp_utils._make_zero()
    OilINDSpec = pp_utils._make_zero()
    MethINDSpec = pp_utils._make_zero()
    EthINDSpec = pp_utils._make_zero()
    GasINDSpec = pp_utils._make_zero()
    CoalINDSpec = pp_utils._make_zero()
    ElecINDSpec = pp.inp('elec_ind-specific', units)
    OnsitePVINDSpec = pp_utils._make_zero()
    DheatINDSpec = pp_utils._make_zero()
    SolThermINDSpec = pp_utils._make_zero()
    H2INDSpec = pp_utils._make_zero()

    vars['Industry Specific|Electricity|Grid'] = ElecINDSpec
    vars['Industry Specific|Electricity|Solar'] = OnsitePVINDSpec
    vars['Industry Specific|Electricity'] = vars['Industry Specific|Electricity|Grid'] + \
        vars['Industry Specific|Electricity|Solar']
    vars['Industry Specific|Gases'] = GasINDSpec
    vars['Industry Specific|Heat'] = DheatINDSpec
    vars['Industry Specific|Hydrogen'] = H2INDSpec
    vars['Industry Specific|Liquids|Biomass'] = EthINDSpec
    vars['Industry Specific|Liquids|Coal'] = MethINDSpec
    vars['Industry Specific|Liquids|Gas'] = pp_utils._make_zero()
    vars['Industry Specific|Liquids|Oil'] = OilINDSpec
    vars['Industry Specific|Liquids'] = vars['Industry Specific|Liquids|Biomass'] + \
        vars['Industry Specific|Liquids|Coal'] + \
        vars['Industry Specific|Liquids|Gas'] + \
        vars['Industry Specific|Liquids|Oil']
    vars['Industry Specific|Other'] = SolThermINDSpec
    vars['Industry Specific|Solids|Coal'] = CoalINDSpec
    vars['Industry Specific|Solids|Biomass'] = BiomassINDSpec
    vars['Industry Specific|Solids'] = vars['Industry Specific|Solids|Coal'] + \
        vars['Industry Specific|Solids|Biomass']

    # Accounting for industry TOTAL
    vars['Industry|Electricity'] = vars['Industry Thermal|Electricity'] + \
        vars['Industry Specific|Electricity']
    vars['Industry|Electricity|Grid'] = vars['Industry Thermal|Electricity|Grid'] + \
        vars['Industry Specific|Electricity|Grid']
    vars['Industry|Electricity|Solar'] = vars['Industry Thermal|Electricity|Solar'] + \
        vars['Industry Specific|Electricity|Solar']
    vars['Industry|Gases'] = vars['Industry Thermal|Gases'] + \
        vars['Industry Specific|Gases']
    vars['Industry|Heat'] = vars['Industry Thermal|Heat'] + \
        vars['Industry Specific|Heat']
    vars['Industry|Hydrogen'] = vars['Industry Thermal|Hydrogen'] + \
        vars['Industry Specific|Hydrogen']
    vars['Industry|Liquids|Biomass'] = vars['Industry Thermal|Liquids|Biomass'] + \
        vars['Industry Specific|Liquids|Biomass']
    vars['Industry|Liquids|Coal'] = vars['Industry Thermal|Liquids|Coal'] + \
        vars['Industry Specific|Liquids|Coal']
    vars['Industry|Liquids|Gas'] = vars['Industry Thermal|Liquids|Gas'] + \
        vars['Industry Specific|Liquids|Gas']
    vars['Industry|Liquids|Oil'] = vars['Industry Thermal|Liquids|Oil'] + \
        vars['Industry Specific|Liquids|Oil']
    vars['Industry|Liquids'] = vars['Industry Thermal|Liquids'] + \
        vars['Industry Specific|Liquids']
    vars['Industry|Other'] = vars['Industry Thermal|Other'] + \
        vars['Industry Specific|Other']
    vars['Industry|Solids|Coal'] = vars['Industry Thermal|Solids|Coal'] + \
        vars['Industry Specific|Solids|Coal']
    vars['Industry|Solids|Biomass'] = vars['Industry Thermal|Solids|Biomass'] + \
        vars['Industry Specific|Solids|Biomass']
    vars['Industry|Solids'] = vars['Industry Thermal|Solids'] + \
        vars['Industry Specific|Solids']

    # Accounting for Residential cooking
    BiomassResCook = pp.inp('bio_cooking', units)
    OilResCook = pp.inp('oil_cooking', units)
    MethResCook = pp_utils._make_zero()
    EthResCook = pp_utils._make_zero()
    GasResCook = pp.inp('gas_cooking', units)
    CoalResCook = pp.inp('coal_cooking', units)
    ElecResCook = pp.inp('elec_cooking', units)
    BiomassNCResCook = pp.inp('traditional chullah', units)

    vars['Residential Cooking|Electricity|Grid'] = ElecResCook
    vars['Residential Cooking|Electricity'] = vars['Residential Cooking|Electricity|Grid']
    vars['Residential Cooking|Gases'] = GasResCook
    vars['Residential Cooking|Other'] = pp_utils._make_zero()
    vars['Residential Cooking|Liquids|Biomass'] = EthResCook
    vars['Residential Cooking|Liquids|Coal'] = MethResCook
    vars['Residential Cooking|Liquids|Gas'] = pp_utils._make_zero()
    vars['Residential Cooking|Liquids|Oil'] = OilResCook
    vars['Residential Cooking|Liquids'] = vars['Residential Cooking|Liquids|Biomass'] + \
        vars['Residential Cooking|Liquids|Coal'] + \
        vars['Residential Cooking|Liquids|Gas'] + \
        vars['Residential Cooking|Liquids|Oil']
    vars['Residential Cooking|Solids|Coal'] = CoalResCook
    vars['Residential Cooking|Solids|Biomass|Modern'] = BiomassResCook
    vars['Residential Cooking|Solids|Biomass|Traditional'] = BiomassNCResCook
    vars['Residential Cooking|Solids|Biomass'] = vars['Residential Cooking|Solids|Biomass|Modern'] + \
        vars['Residential Cooking|Solids|Biomass|Traditional']
    vars['Residential Cooking|Solids'] = vars['Residential Cooking|Solids|Biomass'] + \
        vars['Residential Cooking|Solids|Coal']
    vars['Residential Cooking'] = vars['Residential Cooking|Electricity'] + vars['Residential Cooking|Gases'] + \
        vars['Residential Cooking|Liquids'] + \
        vars['Residential Cooking|Other'] + vars['Residential Cooking|Solids']

    # Accounting for Residential Other
    BiomassResOth = pp_utils._make_zero()
    OilResOth = pp.inp('oil_res-oth', units)
    MethResOth = pp_utils._make_zero()
    EthResOth = pp_utils._make_zero()
    GasResOth = pp_utils._make_zero()
    CoalResOth = pp_utils._make_zero()
    ElecResOth = pp.inp('elec_res-oth', units)
    # Share of offgrid electricity from PV
    OnsitePVResOth = pp.inp('elec-og_res-oth', units) * _share_og_pv
    DheatResOth = pp_utils._make_zero()
    # Share of offgrid electricity from diesel generators
    OtherResOth = pp.inp('elec-og_res-oth', units) * _share_og_dg
    H2ResOth = pp_utils._make_zero()

    vars['Residential Other|Electricity|Grid'] = ElecResOth
    vars['Residential Other|Electricity|Solar'] = OnsitePVResOth
    vars['Residential Other|Electricity|Other'] = OtherResOth
    vars['Residential Other|Electricity'] = vars['Residential Other|Electricity|Grid'] + \
        vars['Residential Other|Electricity|Solar'] + \
        vars['Residential Other|Electricity|Other']
    vars['Residential Other|Gases'] = GasResOth
    vars['Residential Other|Heat|Grid'] = DheatResOth
    vars['Residential Other|Heat'] = vars['Residential Other|Heat|Grid']
    vars['Residential Other|Hydrogen'] = H2ResOth
    vars['Residential Other|Other'] = pp_utils._make_zero()
    vars['Residential Other|Liquids|Biomass'] = EthResOth
    vars['Residential Other|Liquids|Coal'] = MethResOth
    vars['Residential Other|Liquids|Gas'] = pp_utils._make_zero()
    vars['Residential Other|Liquids|Oil'] = OilResOth
    vars['Residential Other|Liquids'] = vars['Residential Other|Liquids|Biomass'] + \
        vars['Residential Other|Liquids|Coal'] + \
        vars['Residential Other|Liquids|Gas'] + \
        vars['Residential Other|Liquids|Oil']
    vars['Residential Other|Solids|Coal'] = CoalResOth
    vars['Residential Other|Solids|Biomass|Modern'] = BiomassResOth
    vars['Residential Other|Solids|Biomass'] = vars['Residential Other|Solids|Biomass|Modern']
    vars['Residential Other|Solids'] = vars['Residential Other|Solids|Biomass'] + \
        vars['Residential Other|Solids|Coal']
    vars['Residential Other'] = vars['Residential Other|Electricity'] + vars['Residential Other|Gases'] + \
        vars['Residential Other|Liquids'] + \
        vars['Residential Other|Other'] + vars['Residential Other|Solids']

    # Accounting for Residential HW
    BiomassResHW = pp_utils._make_zero()
    OilResHW = pp_utils._make_zero()
    MethResHW = pp_utils._make_zero()
    EthResHW = pp_utils._make_zero()
    GasResHW = pp_utils._make_zero()
    CoalResHW = pp_utils._make_zero()
    ElecResHW = pp.inp('elec_res-HW', units)
    OnsitePVResHW = pp_utils._make_zero()
    HeatSolarOffgridResHW = pp.inp('thermal-og_res-HW', units)
    DheatResHW = pp_utils._make_zero()
    OtherResHW = pp_utils._make_zero()
    H2ResHW = pp_utils._make_zero()

    vars['Residential Hotwater|Electricity|Grid'] = ElecResHW-HeatSolarOffgridResHW
    vars['Residential Hotwater|Electricity|Solar'] = OnsitePVResHW
    vars['Residential Hotwater|Electricity|Other'] = OtherResHW
    vars['Residential Hotwater|Electricity'] = vars['Residential Hotwater|Electricity|Grid'] + \
        vars['Residential Hotwater|Electricity|Solar'] + \
        vars['Residential Hotwater|Electricity|Other']
    vars['Residential Hotwater|Gases'] = GasResHW
    vars['Residential Hotwater|Heat|Solar'] = HeatSolarOffgridResHW
    vars['Residential Hotwater|Heat|Grid'] = DheatResHW
    vars['Residential Hotwater|Heat'] = vars['Residential Hotwater|Heat|Solar'] + \
        vars['Residential Hotwater|Heat|Grid']
    vars['Residential Hotwater|Hydrogen'] = H2ResHW
    vars['Residential Hotwater|Other'] = pp_utils._make_zero()
    vars['Residential Hotwater|Liquids|Biomass'] = EthResHW
    vars['Residential Hotwater|Liquids|Coal'] = MethResHW
    vars['Residential Hotwater|Liquids|Gas'] = pp_utils._make_zero()
    vars['Residential Hotwater|Liquids|Oil'] = OilResHW
    vars['Residential Hotwater|Liquids'] = vars['Residential Hotwater|Liquids|Biomass'] + \
        vars['Residential Hotwater|Liquids|Coal'] + \
        vars['Residential Hotwater|Liquids|Gas'] + \
        vars['Residential Hotwater|Liquids|Oil']
    vars['Residential Hotwater|Solids|Coal'] = CoalResHW
    vars['Residential Hotwater|Solids|Biomass|Modern'] = BiomassResHW
    vars['Residential Hotwater|Solids|Biomass'] = vars['Residential Hotwater|Solids|Biomass|Modern']
    vars['Residential Hotwater|Solids'] = vars['Residential Hotwater|Solids|Biomass'] + \
        vars['Residential Hotwater|Solids|Coal']
    vars['Residential Hotwater'] = vars['Residential Hotwater|Electricity'] + vars['Residential Hotwater|Gases'] + \
        vars['Residential Hotwater|Liquids'] + \
        vars['Residential Hotwater|Other'] + \
        vars['Residential Hotwater|Solids']

    # Accounting for Residential TOTAL
    vars['Residential|Electricity|Grid'] = vars['Residential Hotwater|Electricity|Grid'] + \
        vars['Residential Cooking|Electricity|Grid'] + \
        vars['Residential Other|Electricity|Grid']
    vars['Residential|Electricity|Solar'] = vars['Residential Hotwater|Electricity|Solar'] + \
        vars['Residential Other|Electricity|Solar']
    vars['Residential|Electricity|Other'] = vars['Residential Hotwater|Electricity|Other'] + \
        vars['Residential Other|Electricity|Other']
    vars['Residential|Electricity'] = vars['Residential Hotwater|Electricity'] + \
        vars['Residential Cooking|Electricity'] + \
        vars['Residential Other|Electricity']
    vars['Residential|Gases'] = vars['Residential Hotwater|Gases'] + \
        vars['Residential Other|Gases'] + \
        vars['Residential Cooking|Gases']
    vars['Residential|Heat'] = vars['Residential Hotwater|Heat'] + \
        vars['Residential Other|Heat']
    vars['Residential|Heat|Solar'] = vars['Residential Hotwater|Heat|Solar']
    vars['Residential|Heat|Grid'] = vars['Residential Hotwater|Heat|Grid'] + \
        vars['Residential Other|Heat|Grid']
    vars['Residential|Hydrogen'] = vars['Residential Hotwater|Hydrogen'] + \
        vars['Residential Other|Hydrogen']
    vars['Residential|Other'] = vars['Residential Hotwater|Other'] + \
        vars['Residential Cooking|Other'] + vars['Residential Other|Other']
    vars['Residential|Liquids|Biomass'] = vars['Residential Hotwater|Liquids|Biomass'] + \
        vars['Residential Cooking|Liquids|Biomass'] + \
        vars['Residential Other|Liquids|Biomass']
    vars['Residential|Liquids|Coal'] = vars['Residential Hotwater|Liquids|Coal'] + \
        vars['Residential Cooking|Liquids|Coal'] + \
        vars['Residential Other|Liquids|Coal']
    vars['Residential|Liquids|Gas'] = vars['Residential Hotwater|Liquids|Gas'] + \
        vars['Residential Cooking|Liquids|Gas'] + \
        vars['Residential Other|Liquids|Gas']
    vars['Residential|Liquids|Oil'] = vars['Residential Hotwater|Liquids|Oil'] + \
        vars['Residential Cooking|Liquids|Oil'] + \
        vars['Residential Other|Liquids|Oil']
    vars['Residential|Liquids'] = vars['Residential Hotwater|Liquids'] + \
        vars['Residential Cooking|Liquids'] + vars['Residential Other|Liquids']
    vars['Residential|Solids|Coal'] = vars['Residential Hotwater|Solids|Coal'] + \
        vars['Residential Cooking|Solids|Coal'] + \
        vars['Residential Other|Solids|Coal']
    vars['Residential|Solids|Biomass'] = vars['Residential Hotwater|Solids|Biomass'] + \
        vars['Residential Cooking|Solids|Biomass'] + \
        vars['Residential Other|Solids|Biomass']
    vars['Residential|Solids|Biomass|Modern'] = vars['Residential Hotwater|Solids|Biomass|Modern'] + \
        vars['Residential Cooking|Solids|Biomass|Modern'] + \
        vars['Residential Other|Solids|Biomass|Modern']
    vars['Residential|Solids|Biomass|Traditional'] = vars['Residential Cooking|Solids|Biomass|Traditional']
    vars['Residential|Solids'] = vars['Residential Hotwater|Solids'] + \
        vars['Residential Cooking|Solids'] + vars['Residential Other|Solids']
    vars['Residential'] = vars['Residential Hotwater'] + \
        vars['Residential Cooking'] + vars['Residential Other']

    # Accounting for Commercial Other
    BiomassResOth = pp_utils._make_zero()
    OilResOth = pp_utils._make_zero()
    MethResOth = pp_utils._make_zero()
    EthResOth = pp_utils._make_zero()
    GasResOth = pp_utils._make_zero()
    CoalResOth = pp_utils._make_zero()
    ElecResOth = pp.inp('elec_comm-oth', units)
    # Share of offgrid electricity from PV
    OnsitePVResOth = pp.inp('elec-og_comm-oth', units) * _share_og_pv
    DheatResOth = pp_utils._make_zero()
    # Share of offgrid electricity from diesel generators
    OtherResOth = pp.inp('elec-og_comm-oth', units) * _share_og_dg
    H2ResOth = pp_utils._make_zero()

    vars['Commercial Other|Electricity|Grid'] = ElecResOth
    vars['Commercial Other|Electricity|Solar'] = OnsitePVResOth
    vars['Commercial Other|Electricity|Other'] = OtherResOth
    vars['Commercial Other|Electricity'] = vars['Commercial Other|Electricity|Grid'] + \
        vars['Commercial Other|Electricity|Solar'] + \
        vars['Commercial Other|Electricity|Other']
    vars['Commercial Other|Gases'] = GasResOth
    vars['Commercial Other|Heat|Grid'] = DheatResOth
    vars['Commercial Other|Heat'] = vars['Commercial Other|Heat|Grid']
    vars['Commercial Other|Hydrogen'] = H2ResOth
    vars['Commercial Other|Other'] = pp_utils._make_zero()
    vars['Commercial Other|Liquids|Biomass'] = EthResOth
    vars['Commercial Other|Liquids|Coal'] = MethResOth
    vars['Commercial Other|Liquids|Gas'] = pp_utils._make_zero()
    vars['Commercial Other|Liquids|Oil'] = OilResOth
    vars['Commercial Other|Liquids'] = vars['Commercial Other|Liquids|Biomass'] + \
        vars['Commercial Other|Liquids|Coal'] + \
        vars['Commercial Other|Liquids|Gas'] + \
        vars['Commercial Other|Liquids|Oil']
    vars['Commercial Other|Solids|Coal'] = CoalResOth
    vars['Commercial Other|Solids|Biomass'] = BiomassResOth
    vars['Commercial Other|Solids'] = vars['Commercial Other|Solids|Biomass'] + \
        vars['Commercial Other|Solids|Coal']
    vars['Commercial Other'] = vars['Commercial Other|Electricity'] + \
        vars['Commercial Other|Gases'] + \
        vars['Commercial Other|Other'] + vars['Commercial Other|Solids']

    # Accounting for Commercial HW
    BiomassResHW = pp_utils._make_zero()
    OilResHW = pp_utils._make_zero()
    MethResHW = pp_utils._make_zero()
    EthResHW = pp_utils._make_zero()
    GasResHW = pp_utils._make_zero()
    CoalResHW = pp_utils._make_zero()
    ElecResHW = pp.inp('elec_comm-HW', units)
    OnsitePVResHW = pp_utils._make_zero()
    HeatSolarOffgridResHW = pp.inp('thermal-og_comm-HW', units)
    DheatResHW = pp_utils._make_zero()
    OtherResHW = pp_utils._make_zero()

    vars['Commercial Hotwater|Electricity|Grid'] = ElecResHW-HeatSolarOffgridResHW
    vars['Commercial Hotwater|Electricity|Solar'] = OnsitePVResHW
    vars['Commercial Hotwater|Electricity|Other'] = OtherResHW
    vars['Commercial Hotwater|Electricity'] = vars['Commercial Hotwater|Electricity|Grid'] + \
        vars['Commercial Hotwater|Electricity|Solar'] + \
        vars['Commercial Hotwater|Electricity|Other']
    vars['Commercial Hotwater|Gases'] = GasResHW
    vars['Commercial Hotwater|Heat|Solar'] = HeatSolarOffgridResHW
    vars['Commercial Hotwater|Heat|Grid'] = DheatResHW
    vars['Commercial Hotwater|Heat'] = vars['Commercial Hotwater|Heat|Solar'] + \
        vars['Commercial Hotwater|Heat|Grid']
    vars['Commercial Hotwater|Other'] = pp_utils._make_zero()
    vars['Commercial Hotwater|Liquids|Biomass'] = EthResHW
    vars['Commercial Hotwater|Liquids|Coal'] = MethResHW
    vars['Commercial Hotwater|Liquids|Gas'] = pp_utils._make_zero()
    vars['Commercial Hotwater|Liquids|Oil'] = OilResHW
    vars['Commercial Hotwater|Liquids'] = vars['Commercial Hotwater|Liquids|Biomass'] + \
        vars['Commercial Hotwater|Liquids|Coal'] + \
        vars['Commercial Hotwater|Liquids|Gas'] + \
        vars['Commercial Hotwater|Liquids|Oil']
    vars['Commercial Hotwater|Solids|Coal'] = CoalResHW
    vars['Commercial Hotwater|Solids|Biomass'] = BiomassResHW
    vars['Commercial Hotwater|Solids'] = vars['Commercial Hotwater|Solids|Biomass'] + \
        vars['Commercial Hotwater|Solids|Coal']
    vars['Commercial Hotwater'] = vars['Commercial Hotwater|Electricity'] + \
        vars['Commercial Hotwater|Gases'] + \
        vars['Commercial Hotwater|Other'] + vars['Commercial Hotwater|Solids']

    # Accounting for Commercial TOTAL
    vars['Commercial|Electricity|Grid'] = vars['Commercial Other|Electricity|Grid'] + \
        vars['Commercial Hotwater|Electricity|Grid']
    vars['Commercial|Electricity|Solar'] = vars['Commercial Other|Electricity|Solar'] + \
        vars['Commercial Hotwater|Electricity|Solar']
    vars['Commercial|Electricity|Other'] = vars['Commercial Other|Electricity|Other'] + \
        vars['Commercial Hotwater|Electricity|Other']
    vars['Commercial|Electricity'] = vars['Commercial Other|Electricity'] + \
        vars['Commercial Hotwater|Electricity']
    vars['Commercial|Gases'] = vars['Commercial Other|Gases'] + \
        vars['Commercial Hotwater|Gases']
    vars['Commercial|Heat|Solar'] = vars['Commercial Hotwater|Heat|Solar']
    vars['Commercial|Heat|Grid'] = vars['Commercial Other|Heat|Grid'] + \
        vars['Commercial Hotwater|Heat|Grid']
    vars['Commercial|Heat'] = vars['Commercial Other|Heat'] + \
        vars['Commercial Hotwater|Heat']
    vars['Commercial|Hydrogen'] = vars['Commercial Other|Hydrogen']
    vars['Commercial|Other'] = vars['Commercial Other|Other'] + \
        vars['Commercial Hotwater|Other']
    vars['Commercial|Liquids|Biomass'] = vars['Commercial Other|Liquids|Biomass'] + \
        vars['Commercial Hotwater|Liquids|Biomass']
    vars['Commercial|Liquids|Coal'] = vars['Commercial Other|Liquids|Coal'] + \
        vars['Commercial Hotwater|Liquids|Coal']
    vars['Commercial|Liquids|Gas'] = vars['Commercial Other|Liquids|Gas'] + \
        vars['Commercial Hotwater|Liquids|Gas']
    vars['Commercial|Liquids|Oil'] = vars['Commercial Other|Liquids|Oil'] + \
        vars['Commercial Hotwater|Liquids|Oil']
    vars['Commercial|Liquids'] = vars['Commercial Other|Liquids'] + \
        vars['Commercial Hotwater|Liquids']
    vars['Commercial|Solids|Coal'] = vars['Commercial Other|Solids|Coal'] + \
        vars['Commercial Hotwater|Solids|Coal']
    vars['Commercial|Solids|Biomass'] = vars['Commercial Other|Solids|Biomass'] + \
        vars['Commercial Hotwater|Solids|Biomass']
    vars['Commercial|Solids'] = vars['Commercial Other|Solids'] + \
        vars['Commercial Hotwater|Solids']
    vars['Commercial'] = vars['Commercial Other'] + vars['Commercial Hotwater']

    # Accounting for transportation PASSENGER
    ElecPTRPRoadLarge = pp.inp(['elec_LargeP_road'], units)
    ElecPTRPRoadSmall = pp.inp(['elec_SmallP_road'], units)
    ElecPTRPRail = pp.inp(['elec_RAILP'], units)
    OilPTRPRoadLarge = pp.inp(['oil_LargeP_road'], units)
    OilPTRPRoadSmall = pp.inp(['oil_SmallP_road'], units)
    OilPTRPIWTLarge = pp.inp(['oil_LargeP_IWT'], units)
    OilPTRPRail = pp.inp(['oil_RAILP'], units)
    OilPTRPAir = pp.inp(['oil_AIRP'], units)
    MethPTRP = pp_utils._make_zero()
    EthPTRP = pp_utils._make_zero()
    GasPTRPRoadLarge = pp.inp(['gas_LargeP_road'], units)
    GasPTRPRoadSmall = pp.inp(['gas_SmallP_road'], units)
    CoalPTRP = pp_utils._make_zero()
    H2PTRPRoadLarge = pp.inp(['fcv_LargeP_road'], units)
    H2PTRPRoadSmall = pp.inp(['fcv_SmallP_road'], units)

    vars['Transportation Passenger|Electricity|Road|Large'] = ElecPTRPRoadLarge
    vars['Transportation Passenger|Electricity|Road|Small'] = ElecPTRPRoadSmall
    vars['Transportation Passenger|Electricity|Road'] = vars['Transportation Passenger|Electricity|Road|Large'] + \
        vars['Transportation Passenger|Electricity|Road|Small']
    vars['Transportation Passenger|Electricity|Rail'] = ElecPTRPRail
    vars['Transportation Passenger|Electricity'] = vars['Transportation Passenger|Electricity|Road'] + \
        vars['Transportation Passenger|Electricity|Rail']
    vars['Transportation Passenger|Gases|Road|Large'] = GasPTRPRoadLarge
    vars['Transportation Passenger|Gases|Road|Small'] = GasPTRPRoadSmall
    vars['Transportation Passenger|Gases|Road'] = vars['Transportation Passenger|Gases|Road|Large'] + \
        vars['Transportation Passenger|Gases|Road|Small']
    vars['Transportation Passenger|Gases'] = vars['Transportation Passenger|Gases|Road']
    vars['Transportation Passenger|Hydrogen|Road|Large'] = H2PTRPRoadLarge
    vars['Transportation Passenger|Hydrogen|Road|Small'] = H2PTRPRoadSmall
    vars['Transportation Passenger|Hydrogen|Road'] = vars['Transportation Passenger|Hydrogen|Road|Large'] + \
        vars['Transportation Passenger|Hydrogen|Road|Small']
    vars['Transportation Passenger|Hydrogen'] = vars['Transportation Passenger|Hydrogen|Road']
    vars['Transportation Passenger|Liquids|Biomass'] = EthPTRP
    vars['Transportation Passenger|Liquids|Coal'] = MethPTRP
    vars['Transportation Passenger|Liquids|Oil|Road|Large'] = OilPTRPRoadLarge
    vars['Transportation Passenger|Liquids|Oil|Road|Small'] = OilPTRPRoadSmall
    vars['Transportation Passenger|Liquids|Oil|Road'] = vars['Transportation Passenger|Liquids|Oil|Road|Large'] + \
        vars['Transportation Passenger|Liquids|Oil|Road|Small']
    vars['Transportation Passenger|Liquids|Oil|Rail'] = OilPTRPRail
    vars['Transportation Passenger|Liquids|Oil|Aviation'] = OilPTRPAir
    vars['Transportation Passenger|Liquids|Oil|IWT'] = OilPTRPIWTLarge
    vars['Transportation Passenger|Liquids|Oil'] = vars['Transportation Passenger|Liquids|Oil|Road'] + \
        vars['Transportation Passenger|Liquids|Oil|Rail'] + \
        vars['Transportation Passenger|Liquids|Oil|Aviation'] + \
        vars['Transportation Passenger|Liquids|Oil|IWT']
    vars['Transportation Passenger|Liquids|Gas'] = pp_utils._make_zero()
    vars['Transportation Passenger|Liquids'] = vars['Transportation Passenger|Liquids|Biomass'] + \
        vars['Transportation Passenger|Liquids|Coal'] + \
        vars['Transportation Passenger|Liquids|Gas'] + \
        vars['Transportation Passenger|Liquids|Oil']
    vars['Transportation Passenger|Other'] = pp_utils._make_zero()
    vars['Transportation Passenger'] = vars['Transportation Passenger|Electricity'] + vars['Transportation Passenger|Gases'] + \
        vars['Transportation Passenger|Hydrogen'] + \
        vars['Transportation Passenger|Liquids'] + \
        vars['Transportation Passenger|Other']

    # Accounting for transportation FREIGHT
    ElecFTRPRoadHDV = pp_utils._make_zero()
    ElecFTRPRoadLDV = pp_utils._make_zero()
    ElecFTRPRail = pp.inp('elec_RAILF', units)
    OilFTRPRoadHDV = pp.inp(['oil_HDVF_road'], units)
    OilFTRPRoadLDV = pp.inp(['oil_LDVF_road'], units)
    OilFTRPRail = pp.inp(['oil_RAILF'], units)
    OilFTRPAir = pp.inp(['oil_AIRF'], units)
    OilFTRPIWT = pp.inp(['oil_HDVF_IWT'], units)
    MethFTRP = pp_utils._make_zero()
    EthFTRP = pp_utils._make_zero()
    GasFTRPRoadHDV = pp_utils._make_zero()
    GasFTRPRoadLDV = pp_utils._make_zero()
    CoalFTRP = pp_utils._make_zero()
    H2FTRPRoadHDV = pp_utils._make_zero()
    H2FTRPRoadLDV = pp_utils._make_zero()

    vars['Transportation Freight|Electricity|Road|HDV'] = ElecFTRPRoadHDV
    vars['Transportation Freight|Electricity|Road|LDV'] = ElecFTRPRoadLDV
    vars['Transportation Freight|Electricity|Road'] = vars['Transportation Freight|Electricity|Road|HDV'] + \
        vars['Transportation Freight|Electricity|Road|LDV']
    vars['Transportation Freight|Electricity|Rail'] = ElecFTRPRail
    vars['Transportation Freight|Electricity'] = vars['Transportation Freight|Electricity|Road'] + \
        vars['Transportation Freight|Electricity|Rail']
    vars['Transportation Freight|Gases|Road|HDV'] = GasFTRPRoadHDV
    vars['Transportation Freight|Gases|Road|LDV'] = GasFTRPRoadLDV
    vars['Transportation Freight|Gases|Road'] = vars['Transportation Freight|Gases|Road|HDV'] + \
        vars['Transportation Freight|Gases|Road|LDV']
    vars['Transportation Freight|Gases'] = vars['Transportation Freight|Gases|Road']
    vars['Transportation Freight|Hydrogen|Road|HDV'] = H2FTRPRoadHDV
    vars['Transportation Freight|Hydrogen|Road|LDV'] = H2FTRPRoadLDV
    vars['Transportation Freight|Hydrogen|Road'] = vars['Transportation Freight|Hydrogen|Road|HDV'] + \
        vars['Transportation Freight|Hydrogen|Road|LDV']
    vars['Transportation Freight|Hydrogen'] = vars['Transportation Freight|Hydrogen|Road']
    vars['Transportation Freight|Liquids|Biomass'] = EthFTRP
    vars['Transportation Freight|Liquids|Coal'] = MethFTRP
    vars['Transportation Freight|Liquids|Oil|Road|HDV'] = OilFTRPRoadHDV
    vars['Transportation Freight|Liquids|Oil|Road|LDV'] = OilFTRPRoadLDV
    vars['Transportation Freight|Liquids|Oil|Road'] = vars['Transportation Freight|Liquids|Oil|Road|HDV'] + \
        vars['Transportation Freight|Liquids|Oil|Road|LDV']
    vars['Transportation Freight|Liquids|Oil|Rail'] = OilFTRPRail
    vars['Transportation Freight|Liquids|Oil|Aviation'] = OilFTRPAir
    vars['Transportation Freight|Liquids|Oil|IWT'] = OilFTRPIWT
    vars['Transportation Freight|Liquids|Oil'] = vars['Transportation Freight|Liquids|Oil|Road'] + \
        vars['Transportation Freight|Liquids|Oil|Rail'] + \
        vars['Transportation Freight|Liquids|Oil|Aviation'] + \
        vars['Transportation Freight|Liquids|Oil|IWT']
    vars['Transportation Freight|Liquids|Gas'] = pp_utils._make_zero()
    vars['Transportation Freight|Liquids'] = vars['Transportation Freight|Liquids|Biomass'] + \
        vars['Transportation Freight|Liquids|Coal'] + \
        vars['Transportation Freight|Liquids|Gas'] + \
        vars['Transportation Freight|Liquids|Oil']
    vars['Transportation Freight|Other'] = pp_utils._make_zero()
    vars['Transportation Freight'] = vars['Transportation Freight|Electricity'] + vars['Transportation Freight|Gases'] + \
        vars['Transportation Freight|Hydrogen'] + \
        vars['Transportation Freight|Liquids'] + \
        vars['Transportation Freight|Other']

    # Accounting for Transportation TOTAL
    vars['Transportation|Electricity|Road|Large'] = vars['Transportation Passenger|Electricity|Road|Large'] + \
        vars['Transportation Freight|Electricity|Road|HDV']
    vars['Transportation|Electricity|Road|Small'] = vars['Transportation Passenger|Electricity|Road|Small'] + \
        vars['Transportation Freight|Electricity|Road|LDV']
    vars['Transportation|Electricity|Road'] = vars['Transportation Passenger|Electricity|Road'] + \
        vars['Transportation Freight|Electricity|Road']
    vars['Transportation|Electricity|Rail'] = vars['Transportation Passenger|Electricity|Rail'] + \
        vars['Transportation Freight|Electricity|Rail']
    vars['Transportation|Electricity'] = vars['Transportation Passenger|Electricity'] + \
        vars['Transportation Freight|Electricity']
    vars['Transportation|Gases|Road|Large'] = vars['Transportation Passenger|Gases|Road|Large'] + \
        vars['Transportation Freight|Gases|Road|HDV']
    vars['Transportation|Gases|Road|Small'] = vars['Transportation Passenger|Gases|Road|Small'] + \
        vars['Transportation Freight|Gases|Road|LDV']
    vars['Transportation|Gases|Road'] = vars['Transportation Passenger|Gases|Road'] + \
        vars['Transportation Freight|Gases|Road']
    vars['Transportation|Gases'] = vars['Transportation Passenger|Gases'] + \
        vars['Transportation Freight|Gases']
    vars['Transportation|Hydrogen|Road|Large'] = vars['Transportation Passenger|Hydrogen|Road|Large'] + \
        vars['Transportation Freight|Hydrogen|Road|HDV']
    vars['Transportation|Hydrogen|Road|Small'] = vars['Transportation Passenger|Hydrogen|Road|Small'] + \
        vars['Transportation Freight|Hydrogen|Road|LDV']
    vars['Transportation|Hydrogen|Road'] = vars['Transportation Passenger|Hydrogen|Road'] + \
        vars['Transportation Freight|Hydrogen|Road']
    vars['Transportation|Hydrogen'] = vars['Transportation Passenger|Hydrogen'] + \
        vars['Transportation Freight|Hydrogen']
    vars['Transportation|Liquids|Biomass'] = vars['Transportation Passenger|Liquids|Biomass'] + \
        vars['Transportation Freight|Liquids|Biomass']
    vars['Transportation|Liquids|Coal'] = vars['Transportation Passenger|Liquids|Coal'] + \
        vars['Transportation Freight|Liquids|Coal']
    vars['Transportation|Liquids|Oil|Road|Large'] = vars['Transportation Passenger|Liquids|Oil|Road|Large'] + \
        vars['Transportation Freight|Liquids|Oil|Road|HDV']
    vars['Transportation|Liquids|Oil|Road|Small'] = vars['Transportation Passenger|Liquids|Oil|Road|Small'] + \
        vars['Transportation Freight|Liquids|Oil|Road|LDV']
    vars['Transportation|Liquids|Oil|Road'] = vars['Transportation Passenger|Liquids|Oil|Road'] + \
        vars['Transportation Freight|Liquids|Oil|Road']
    vars['Transportation|Liquids|Oil|Rail'] = vars['Transportation Passenger|Liquids|Oil|Rail'] + \
        vars['Transportation Freight|Liquids|Oil|Rail']
    vars['Transportation|Liquids|Oil|Aviation'] = vars['Transportation Passenger|Liquids|Oil|Aviation'] + \
        vars['Transportation Freight|Liquids|Oil|Aviation']
    vars['Transportation|Liquids|Oil|IWT'] = vars['Transportation Passenger|Liquids|Oil|IWT'] + \
        vars['Transportation Freight|Liquids|Oil|IWT']
    vars['Transportation|Liquids|Oil'] = vars['Transportation Passenger|Liquids|Oil'] + \
        vars['Transportation Freight|Liquids|Oil']
    vars['Transportation|Liquids|Gas'] = vars['Transportation Passenger|Liquids|Gas'] + \
        vars['Transportation Freight|Liquids|Gas']
    vars['Transportation|Liquids'] = vars['Transportation Passenger|Liquids'] + \
        vars['Transportation Freight|Liquids']
    vars['Transportation|Other'] = vars['Transportation Passenger|Other'] + \
        vars['Transportation Freight|Other']
    vars['Transportation'] = vars['Transportation Passenger'] + \
        vars['Transportation Freight']

    # Accounting for Agricultural sector
    BiomassAgri = pp_utils._make_zero()
    OilAgriPump = pp.inp('oil_agri-pump', units)
    OilAgriTractor = pp.inp('oil_agri-trp', units)
    MethAgri = pp_utils._make_zero()
    EthAgri = pp_utils._make_zero()
    GasAgri = pp_utils._make_zero()
    CoalAgri = pp_utils._make_zero()
    ElecAgri = pp.inp('elec_agri-pump', units)
    # Share of offgrid electricity from PV
    OnsitePVAgri = pp.inp('elec-og_agri-pump', units) * _share_og_pv
    DheatAgri = pp_utils._make_zero()
    # Share of offgrid electricity from diesel generators
    OtherAgri = pp.inp('elec-og_agri-pump', units) * _share_og_dg
    H2Agri = pp_utils._make_zero()

    vars['Agriculture|Electricity|Grid'] = ElecAgri
    vars['Agriculture|Electricity|Solar'] = OnsitePVAgri
    vars['Agriculture|Electricity|Other'] = OtherAgri
    vars['Agriculture|Electricity'] = vars['Agriculture|Electricity|Grid'] + \
        vars['Agriculture|Electricity|Solar'] + \
        vars['Agriculture|Electricity|Other']
    vars['Agriculture|Gases'] = GasAgri
    vars['Agriculture|Heat|Grid'] = DheatAgri
    vars['Agriculture|Heat'] = vars['Agriculture|Heat|Grid']
    vars['Agriculture|Hydrogen'] = H2Agri
    vars['Agriculture|Other'] = pp_utils._make_zero()
    vars['Agriculture|Liquids|Biomass'] = EthAgri
    vars['Agriculture|Liquids|Coal'] = MethAgri
    vars['Agriculture|Liquids|Gas'] = pp_utils._make_zero()
    vars['Agriculture|Liquids|Oil|Pump'] = OilAgriPump
    vars['Agriculture|Liquids|Oil|Tractors'] = OilAgriTractor
    vars['Agriculture|Liquids|Oil'] = vars['Agriculture|Liquids|Oil|Pump'] + \
        vars['Agriculture|Liquids|Oil|Tractors']
    vars['Agriculture|Liquids'] = vars['Agriculture|Liquids|Biomass'] + \
        vars['Agriculture|Liquids|Coal'] + \
        vars['Agriculture|Liquids|Gas'] + \
        vars['Agriculture|Liquids|Oil']
    vars['Agriculture|Solids|Coal'] = CoalAgri
    vars['Agriculture|Solids|Biomass|Modern'] = BiomassAgri
    vars['Agriculture|Solids|Biomass'] = vars['Agriculture|Solids|Biomass|Modern']
    vars['Agriculture|Solids'] = vars['Agriculture|Solids|Biomass'] + \
        vars['Agriculture|Solids|Coal']
    vars['Agriculture'] = vars['Agriculture|Electricity'] + vars['Agriculture|Gases'] + \
        vars['Agriculture|Liquids'] + \
        vars['Agriculture|Other'] + vars['Agriculture|Solids']

    # Provisional accounting for non-parent variables
    vars['Electricity|Grid'] = vars['Transportation|Electricity'] + vars['Commercial|Electricity|Grid'] + \
        vars['Residential|Electricity|Grid'] + \
        vars['Agriculture|Electricity|Grid'] + \
        vars['Industry Thermal|Electricity|Grid'] + \
        vars['Industry Specific|Electricity|Grid']
    vars['Electricity|Other'] = vars['Commercial|Electricity|Other'] + \
        vars['Residential|Electricity|Other'] + \
        vars['Agriculture|Electricity|Other']
    vars['Electricity|Solar'] = vars['Commercial|Electricity|Solar'] + \
        vars['Residential|Electricity|Solar'] + \
        vars['Agriculture|Electricity|Solar'] + \
        vars['Industry Thermal|Electricity|Solar'] + \
        vars['Industry Specific|Electricity|Solar']
    vars['Electricity'] = vars['Electricity|Grid'] + \
        vars['Electricity|Other'] + vars['Electricity|Solar']
    vars['Gases'] = vars['Transportation|Gases'] + vars['Commercial|Gases'] + \
        vars['Residential|Gases'] + vars['Agriculture|Gases'] + vars['Industry Thermal|Gases'] + \
        vars['Industry Specific|Gases']
    vars['Heat|Grid'] = vars['Commercial|Heat|Grid'] + \
        vars['Residential|Heat|Grid'] + vars['Agriculture|Heat|Grid'] + vars['Industry Thermal|Heat'] + \
        vars['Industry Specific|Heat']
    vars['Heat|Solar'] = vars['Commercial|Heat|Solar'] + \
        vars['Residential|Heat|Solar']
    vars['Heat'] = vars['Heat|Grid'] + vars['Heat|Solar']
    vars['Hydrogen'] = vars['Transportation|Hydrogen'] + vars['Commercial|Hydrogen'] + \
        vars['Residential|Hydrogen'] + vars['Agriculture|Hydrogen'] + vars['Industry Thermal|Hydrogen'] + \
        vars['Industry Specific|Hydrogen']
    vars['Liquids|Biomass'] = vars['Transportation|Liquids|Biomass'] + vars['Commercial|Liquids|Biomass'] + \
        vars['Residential|Liquids|Biomass'] + \
        vars['Agriculture|Liquids|Biomass'] + \
        vars['Industry Thermal|Liquids|Biomass'] + \
        vars['Industry Specific|Liquids|Biomass']
    vars['Liquids|Coal'] = vars['Transportation|Liquids|Coal'] + vars['Commercial|Liquids|Coal'] + \
        vars['Residential|Liquids|Coal'] + vars['Agriculture|Liquids|Coal'] + vars['Industry Thermal|Liquids|Coal'] + \
        vars['Industry Specific|Liquids|Coal']
    vars['Liquids|Gas'] = vars['Transportation|Liquids|Gas'] + vars['Commercial|Liquids|Gas'] + \
        vars['Residential|Liquids|Gas'] + vars['Agriculture|Liquids|Gas'] + vars['Industry Thermal|Liquids|Gas'] + \
        vars['Industry Specific|Liquids|Gas']
    vars['Liquids|Oil'] = vars['Transportation|Liquids|Oil'] + vars['Commercial|Liquids|Oil'] + \
        vars['Residential|Liquids|Oil'] + vars['Agriculture|Liquids|Oil'] + vars['Industry Thermal|Liquids|Oil'] + \
        vars['Industry Specific|Liquids|Oil']
    vars['Liquids'] = vars['Liquids|Biomass'] + \
        vars['Liquids|Coal'] + vars['Liquids|Gas'] + vars['Liquids|Oil']
    vars['Solar'] = vars['Electricity|Solar'] + vars['Heat|Solar']
    vars['Solids|Biomass|Traditional'] = vars['Residential|Solids|Biomass|Traditional']
    vars['Solids|Biomass|Modern'] = vars['Commercial|Solids|Biomass'] + \
        vars['Residential|Solids|Biomass|Modern'] + \
        vars['Agriculture|Solids|Biomass|Modern'] + \
        vars['Industry Thermal|Solids|Biomass'] + \
        vars['Industry Specific|Solids|Biomass']
    vars['Solids|Biomass'] = vars['Solids|Biomass|Modern'] + \
        vars['Solids|Biomass|Traditional']
    vars['Solids|Coal'] = vars['Commercial|Solids|Coal'] + \
        vars['Residential|Solids|Coal'] + vars['Agriculture|Solids|Coal'] + vars['Industry Thermal|Solids|Coal'] + \
        vars['Industry Specific|Solids|Coal']
    vars['Other'] = vars['Transportation|Other'] + vars['Commercial|Other'] + \
        vars['Residential|Other'] + vars['Agriculture|Other']
#
#    # Additonal reporting for GAINS diagnostic linkage in the CD_Links scenario process
#    # As non energy use we report the following categories: Feedstocks
#
#    vars['Non-Energy Use|Biomass'] = pp.inp(['ethanol_fs'], units, inpfilter={
#        'commodity': ['ethanol']})
#    # Note - this category includes both coal from a solid and liquid
#    # (methanol) source
#    vars['Non-Energy Use|Coal'] = pp.inp(['coal_fs', 'methanol_fs'], units, inpfilter={
#        'commodity': ['coal', 'methanol']})
#    # Note - this can include biogas, natural gas and hydrogen which is mixed
#    # further upstream
#    vars['Non-Energy Use|Gas'] = pp.inp(['gas_fs'], units, inpfilter={
#        'commodity': ['gas']})
#    vars[
#        'Non-Energy Use|Oil'] = pp.inp(['foil_fs', 'loil_fs'], units, inpfilter={
#            'commodity': ['fueloil', 'lightoil']})
#    vars['Geothermal'] = pp_utils._make_zero()

#    Currently, the split required to run GAINS between Commercial and Residential is not supported.
#    vars['Commercial|Electricity'] =
#    vars['Commercial|Gases'] =
#    vars['Commercial|Heat'] =
#    vars['Commercial|Hydrogen'] =
#    vars['Commercial|Liquids'] =
#    vars['Commercial|Solids'] =
#    vars['Commercial|Solids|Biomass'] =
#    vars['Commercial|Solids|Biomass|Traditional'] =
#    vars['Commercial|Solids|Coal'] =
#    vars['Residential|Electricity'] =
#    vars['Residential|Gases'] =
#    vars['Residential|Heat'] =
#    vars['Residential|Hydrogen'] =
#    vars['Residential|Liquids'] =
#    vars['Residential|Solids'] =
#    vars['Residential|Solids|Biomass'] =
#    vars['Residential|Solids|Biomass|Traditional'] =
#    vars['Residential|Solids|Coal'] =

#    Currently, the split required to run GAINS to differentiate between fraight and Passenger Transport is not supported.
#    vars['Transportation|Freight'] =
#    vars['Transportation|Freight|Electricity'] =
#    vars['Transportation|Freight|Gases'] =
#    vars['Transportation|Freight|Hydrogen'] =
#    vars['Transportation|Freight|Liquids'] =
#    vars['Transportation|Freight|Liquids|Biomass'] =
#    vars['Transportation|Freight|Liquids|Oil'] =
#    vars['Transportation|Liquids|Natural Gas'] =
#    vars['Transportation|Passenger'] =
#    vars['Transportation|Passenger|Electricity'] =
#    vars['Transportation|Passenger|Gases'] =
#    vars['Transportation|Passenger|Hydrogen'] =
#    vars['Transportation|Passenger|Liquids'] =
#    vars['Transportation|Passenger|Liquids|Biomass'] =
#    vars['Transportation|Passenger|Liquids|Oil'] =

    df = pp_utils.make_outputdf(vars, units, model, scenario, glb=False)
    return(df)


def retr_supply_inv(model, scenario, pp, pp_utils):
    vars = {}
    units = 'billion USD$2010/yr'

    # Comment OFR - 20.04.2017: The following has been checked between Volker
    # Krey, David McCollum and Oliver Fricko

    # There are fixed factors by which ccs technologies are multiplied which equate to the share of the powerplant costs which split investments into the
    # share associated with the standard powerplant and the share associated
    # with those investments related to CCS

    # For some extraction and synfuel technologies, a certain share of the voms and foms are attributed to the investments which is based on the GEA-Study
    # where the derived investment costs were partly attributed to the
    # voms/foms

    # Extraction
    # COMMENT OFR 25.04.2017: All non-extraction costs for Coal, Gas and Oil
    # have been moved to 'Energy|Other'
    vars['Extraction|Coal'] = pp.investment(['coal_extr'], units=units) + pp.act_vom(['coal_extr'], units=units)
#     vars['Extraction|Gas'] = pp.investment(['gas_extr_1', 'gas_extr_2', 'gas_extr_3', 'gas_extr_4', 'gas_extr_5', 'gas_extr_6', 'gas_extr_7', 'gas_extr_8'], units=units) + pp.act_vom([
#         'gas_extr_1', 'gas_extr_2', 'gas_extr_3', 'gas_extr_4', 'gas_extr_5', 'gas_extr_6', 'gas_extr_7', 'gas_extr_8'], units=units) * .5
    # COMMENT OFR 25.04.2017: Any costs relating to refineries have been
    # removed (compared to GEA) as these are reported under 'Liquids|Oil'
#     vars['Extraction|Oil'] = pp.investment(['oil_extr_1', 'oil_extr_2', 'oil_extr_3', 'oil_extr_1_ch4', 'oil_extr_2_ch4', 'oil_extr_3_ch4', 'oil_extr_4', 'oil_extr_4_ch4', 'oil_extr_5', 'oil_extr_6', 'oil_extr_7', 'oil_extr_8'],
#                                            units=units) + pp.act_vom(['oil_extr_1', 'oil_extr_2', 'oil_extr_3', 'oil_extr_1_ch4', 'oil_extr_2_ch4', 'oil_extr_3_ch4', 'oil_extr_4', 'oil_extr_4_ch4', 'oil_extr_5', 'oil_extr_6', 'oil_extr_7', 'oil_extr_8'], units=units)

    # As no mode is specified, u5-reproc will account for all 3 modes.
#     vars['Extraction|Uranium'] = pp.investment(['uran2u5', 'Uran_extr', 'u5-reproc', 'plutonium_prod'], units=units) \
#         + pp.act_vom(['uran2u5', 'Uran_extr'], units=units) \
#         + pp.act_vom(['u5-reproc'], actfilter={'mode': ['M1']}, units=units)\
#         + pp.investment(['uran2u5', 'Uran_extr', 'u5-reproc',
#                          'plutonium_prod'], units=units)

    # QUESTION: There are no speicifc costs allocated only to biomass extraction. The investments/costs are currently reflecting the price per unit of biomass and the costs of land use change required to mitigate total AFOLU GHG mitigation
    # vars['Extraction|Biomass']

    # Electricity - Fossils
#     vars['Electricity|Coal|w/ CCS'] = pp.investment(['c_ppl_co2scr', 'cfc_co2scr'], units=units) + pp.investment(
#         'coal_adv_ccs', units=units) * 0.25 + pp.investment('igcc_ccs', units=units) * 0.31
    vars['Electricity|Coal|w/o CCS'] = pp.investment(['coal_ppl', 'coal_ppl_sub'], units=units) + pp.investment(
        'coal_usc_ccs', units=units) * 0.75 + pp.investment('igcc', units=units) + pp.investment('igcc_ccs', units=units) * 0.69
#     vars['Electricity|Gas|w/ CCS'] = pp.investment(
#         ['g_ppl_co2scr', 'gfc_co2scr'], units=units) + pp.investment('gas_cc_ccs', units=units) * 0.53
#     vars['Electricity|Gas|w/o CCS'] = pp.investment(
#         ['gas_cc', 'gas_ct', 'gas_ppl'], units=units) + pp.investment('gas_cc_ccs', units=units) * 0.47
# #     vars['Electricity|Oil|w/o CCS'] = pp.investment(
#         ['foil_ppl', 'loil_ppl', 'oil_ppl', 'loil_cc'], units=units)

    # Electricity - Renewables
#     vars['Electricity|Biomass|w/ CCS'] = pp.investment(
#         'bio_ppl_co2scr', units=units) + pp.investment('bio_istig_ccs', units=units) * 0.31
    vars['Electricity|Biomass|w/o CCS'] = pp.investment(
        ['bio_ppl'], units=units) + pp.investment('bio_ppl', units=units) * 0.69
#     vars['Electricity|Geothermal'] = pp.investment('geo_ppl', units=units)
    vars['Electricity|Hydro'] = pp.investment(
        ['hydro'], units=units)

#     vars['Electricity|Other'] = pp.investment(
#         ['h2_fc_I', 'h2_fc_RC'], units=units)
    _solar_pv_elec = pp.investment(
        ['solar_PV'], units=units)
    _solar_th_elec = pp.investment(['solar_CSP'], units=units)
    vars['Electricity|Solar Grid'] = _solar_pv_elec + _solar_th_elec
    vars['Electricity|Solar RPO'] = pp.act_vom(['solar_RPO','solar_PV_offgrid'], units=units)
    vars['Offgrid Renewable|Solar PV'] = pp.investment(['solar_PV_offgrid'], units=units)
    vars['Offgrid Renewable|Solar Thermal'] = pp.investment(['solar_thermal-HW_offgrid'], units=units)
    vars['Offgrid Renewable'] = vars['Offgrid Renewable|Solar Thermal'] + vars['Offgrid Renewable|Solar PV']
    vars['Electricity|Wind'] = pp.investment(['wind_offshore','wind_onshore'], units=units)

    # Electricity Storage, transmission and distribution
#     vars['Electricity|Electricity Storage'] = pp.investment(
#         'stor_ppl', units=units)

    # Electricity Nuclear
#     vars['Electricity|Nuclear'] = pp.investment(
#         ['nuc_hc', 'nuc_lc'], units=units)

    # COMMENT OFR: Pre-ix, the voms were multiplied with the output
    # ['elec_t_d','elec_exp','elec_imp']. This has been changed to the
    # activity
    vars['Electricity|Transmission and Distribution'] = pp.investment(['elec_grid', 'elec_exp', 'elec_imp'], units=units) + pp.act_vom(
        ['elec_grid','elec_exp', 'elec_imp'], units=units) * .5 + pp.tic_fom(['elec_grid', 'elec_exp', 'elec_imp'], units=units) * .5

#     # CO2 Storage, transmission and distribution
#     emission_units = 'Mt CO2/yr'
#     _CCS_coal_elec = -1 * pp.emi(['c_ppl_co2scr', 'coal_adv_ccs', 'igcc_ccs', 'cement_co2scr'], 'GWa', emifilter={
#         'relation': ['CO2_Emission']}, emission_units=emission_units)
#     _CCS_coal_synf = -1 * pp.emi(['syn_liq_ccs', 'h2_coal_ccs', 'meth_coal_ccs'], 'GWa', emifilter={
#         'relation': ['CO2_Emission']}, emission_units=emission_units)
#     _CCS_gas_elec = -1 * pp.emi(['g_ppl_co2scr', 'gas_cc_ccs'], 'GWa', emifilter={
#         'relation': ['CO2_Emission']}, emission_units=emission_units)
#     _CCS_gas_synf = -1 * pp.emi(['h2_smr_ccs', 'meth_ng_ccs'], 'GWa', emifilter={
#         'relation': ['CO2_Emission']}, emission_units=emission_units)
#     _CCS_bio_elec = -1 * pp.emi(['bio_ppl_co2scr', 'bio_istig_ccs'], 'GWa', emifilter={
#         'relation': ['CO2_Emission']}, emission_units=emission_units)
#     _CCS_bio_synf = -1 * pp.emi(['eth_bio_ccs', 'liq_bio_ccs', 'h2_bio_ccs'], 'GWa', emifilter={
#         'relation': ['CO2_Emission']}, emission_units=emission_units)
#     _Biogas_use_tot = pp.out('gas_bio')
#     _Gas_use_tot = pp.inp(['gas_ppl', 'gas_cc', 'gas_cc_ccs', 'gas_ct', 'gas_htfc', 'gas_hpl', 'meth_ng',
#                            'meth_ng_ccs', 'h2_smr', 'h2_smr_ccs', 'gas_rc', 'hp_gas_rc', 'gas_i', 'hp_gas_i', 'gas_trp', 'gas_fs'], inpfilter={'commodity': ['biomass']})
#     _Biogas_share = (_Biogas_use_tot / _Gas_use_tot).fillna(0)
#     _CCS_Foss = _CCS_coal_elec + _CCS_coal_synf + _CCS_gas_elec * \
#         (1 - _Biogas_share) + _CCS_gas_synf * (1 - _Biogas_share)
#     _CCS_Bio = _CCS_bio_elec + _CCS_bio_synf - \
#         (_CCS_gas_elec + _CCS_gas_synf) * _Biogas_share
#     _CCS_coal_elec_shr = (_CCS_coal_elec / _CCS_Foss).fillna(0)
#     _CCS_coal_synf_shr = (_CCS_coal_synf / _CCS_Foss).fillna(0)
#     _CCS_gas_elec_shr = (_CCS_gas_elec / _CCS_Foss).fillna(0)
#     _CCS_gas_synf_shr = (_CCS_gas_synf / _CCS_Foss).fillna(0)
#     _CCS_bio_elec_shr = (_CCS_bio_elec / _CCS_Bio).fillna(0)
#     _CCS_bio_synf_shr = (_CCS_bio_synf / _CCS_Bio).fillna(0)

#     CO2_trans_dist_elec = pp.act_vom('co2_tr_dis', units=units) * 0.5 * _CCS_coal_elec_shr + pp.act_vom(
#         'co2_tr_dis', units=units) * 0.5 * _CCS_gas_elec_shr + pp.act_vom('bco2_tr_dis', units=units) * 0.5 * _CCS_bio_elec_shr
#     CO2_trans_dist_synf = pp.act_vom('co2_tr_dis', units=units) * 0.5 * _CCS_coal_synf_shr + pp.act_vom(
#         'co2_tr_dis', units=units) * 0.5 * _CCS_gas_synf_shr + pp.act_vom('bco2_tr_dis', units=units) * 0.5 * _CCS_bio_synf_shr
#     vars['CO2 Transport and Storage'] = CO2_trans_dist_elec + CO2_trans_dist_synf

    # Heat
#     vars['Heat'] = pp.investment(
#         ['coal_hpl', 'foil_hpl', 'gas_hpl', 'bio_hpl', 'heat_t_d', 'po_turbine'], units=units)

    # Synthetic fuel production
    # COMMENT OFR 25.04.2017: XXX_synf_ccs has been split into hydrogen and
    # liquids. The shares then add up to 1, but the variables are kept
    # separate in order to preserve the split between CCS and non-CCS
#     _Coal_synf_ccs_liq = pp.investment(
#         'meth_coal_ccs', units=units) * 0.02 + pp.investment('syn_liq_ccs', units=units) * 0.01
#     _Gas_synf_ccs_liq = pp.investment('meth_ng_ccs', units=units) * 0.08
#     _Bio_synf_ccs_liq = pp.investment(
#         'eth_bio_ccs', units=units) * 0.34 + pp.investment('liq_bio_ccs', units=units) * 0.02
#     _Coal_synf_ccs_h2 = pp.investment('h2_coal_ccs', units=units) * 0.03
#     _Gas_synf_ccs_h2 = pp.investment('h2_smr_ccs', units=units) * 0.17
#     _Bio_synf_ccs_h2 = pp.investment('h2_bio_ccs', units=units) * 0.02

    # COMMENT OFR 25.04.2017: 'coal_gas' have been moved to 'other'
#     vars['Liquids|Coal and Gas'] = pp.investment(['meth_coal', 'syn_liq', 'meth_ng'], units=units) + pp.investment('meth_ng_ccs', units=units) * 0.92 + pp.investment(
#         'meth_coal_ccs', units=units) * 0.98 + pp.investment('syn_liq_ccs', units=units) * 0.99 + _Coal_synf_ccs_liq + _Gas_synf_ccs_liq
#     # COMMENT OFR 25.04.2017: 'gas_bio' has been moved to 'other'
#     vars['Liquids|Biomass'] = pp.investment(['eth_bio', 'liq_bio'], units=units) + pp.investment(
#         'liq_bio_ccs', units=units) * 0.98 + pp.investment('eth_bio_ccs', units=units) * 0.66 + _Bio_synf_ccs_liq
#     # COMMENT OFR 25.04.2017: 'transport, import and exports costs related to
#     # liquids are only included in the total'
#     _Synfuel_other = pp.investment(['meth_exp', 'meth_imp', 'meth_t_d', 'meth_bal',
#                                     'eth_exp', 'eth_imp', 'eth_t_d', 'eth_bal', 'SO2_scrub_synf'], units=units)
#     vars['Liquids|Oil'] = pp.investment(
#         ['ref_lol', 'ref_hil'], units=units) + pp.tic_fom(['ref_hil', 'ref_lol'], units=units)
#     vars['Liquids'] = vars['Liquids|Coal and Gas'] + \
#         vars['Liquids|Biomass'] + vars['Liquids|Oil'] + _Synfuel_other

    # Hydrogen
#     vars['Hydrogen|Fossil'] = pp.investment(['h2_coal', 'h2_smr'], units=units) + pp.investment(
#         'h2_coal_ccs', units=units) * 0.97 + pp.investment('h2_smr_ccs', units=units) * 0.83 + _Coal_synf_ccs_h2 + _Gas_synf_ccs_h2
#     vars['Hydrogen|Renewable'] = pp.investment(
#         'h2_bio', units=units) + pp.investment('h2_bio_ccs', units=units) * 0.98 + _Bio_synf_ccs_h2
#     vars['Hydrogen|Other'] = pp.investment(['h2_elec', 'h2_liq', 'h2_t_d', 'lh2_exp', 'lh2_imp',
#                                             'lh2_bal', 'lh2_regas', 'lh2_t_d'], units=units) + pp.act_vom('h2_mix', units=units) * 0.5

    # All questionable variables from extraction that are not related directly
    # to extraction should be moved to Other
    # COMMENT OFR 25.04.2017: Any costs relating to refineries have been
    # removed (compared to GEA) as these are reported under 'Liquids|Oil'
    
    vars['Others|Coal Balance & Imports'] = pp.investment(['coal_imp', 'coal_bal', 'coal_t/d'], units=units) + pp.act_vom(['coal_t/d'], units=units) * 0.5
    vars['Others|Gas'] = pp.act_vom(['gas_imp'], units=units) + pp.investment(['gas_imp', 'gas_t/d','gas_import_terminal'], units=units)
    vars['Others|Oil'] = pp.act_vom(['oil_imp'], units=units) + pp.investment(['oil_imp', 'oil_t/d','oil_import_terminal'], units=units)
    vars['Others|Biomass'] = pp.investment('bio_t/d', units=units)
    vars['Others'] =vars['Others|Coal Balance & Imports'] + vars['Others|Oil'] + vars['Others|Gas'] + vars['Others|Biomass']
    df = pp_utils.make_outputdf(vars, units, model, scenario, glb=False)
    return(df)


# Switches to run different cin files
# Energy related
resource_extraction = 1
resource_cumulative_extraction = 1
primary_energy = 1
SE_electricity_generation = 1
SE_district_heat = 0
SE_synfuels = 0
SE_gases = 1
SE_solids = 1
final_energy = 1

# Emissions related
CO2_CCS = 1
CO2_emissions = 1
other_emissions = 0  # BC, CH4, CO, N2O, NH3, NOx, OC, Sulfur, VOC Checked - dont change

# Parameters
total_installed_capacity = 1
new_installed_capacity = 1
cumulative_installed_capacity = 1
capital_cost = 1
fix_cost = 1
var_cost = 1
tech_lifetime = 1
efficiency = 1
supply_side_investments = 1

# Other indicators
population = 0
prices = 1
demands = 1


def report(ix_mp, ds, ref_sol, model, scenario, ix_mdl_nm=False, ix_scn_nm=False, version=None, db_upload=False, merge_hist=False, out_dir=None):

    xlsx = 'MESSAGEix_WorkDB_Template.xlsx'

    if not ix_mdl_nm:
        ix_mdl_nm = {model: model}
    if not ix_scn_nm:
        ix_scn_nm = {scenario: scenario}

    dfs = []
    # get the datastructure for a single model and scenario

    if ref_sol != 'True':
        pp = postprocess.PostProcess(ds)
    else:
        pp = postprocess.PostProcess(ds, ix=False)

    if ref_sol != 'True':
        firstmodelyear = int(ds.set("cat_year", filters={
            'type_year': ['firstmodelyear']})['year'])
        pp_utils.firstmodelyear = firstmodelyear

        years = [int(y)
                 for y in ds.set('year').values if int(y) >= firstmodelyear]
        pp_utils.years = years
    else:
        years = [int(y) for y in ds.set('year').values if int(y) >= ds.set(
            "cat_year", filters={'type_year': 'firstmodelyear'})['year'][0]]
        pp_utils.years = years

    regions = {
        'India': 'India',
    }
    pp_utils.regions = regions

    # TODO - Insert a function that retrieves and sets years
    # set other global variables in pp_utils
    pp_utils.all_tecs = ds.set('technology')

    # Resource Extraction
    if resource_extraction:
        what = 'Resource|Extraction'
        print('processing Table:', what)
        dfs.append(pp_utils.iamc_it(retr_extraction(
            ix_mdl_nm[model], ix_scn_nm[scenario], pp, pp_utils), what, xlsx))

    # Cumulative resource Extraction
    if resource_cumulative_extraction:
        what = 'Resource|Cumulative Extraction'
        print('processing Table:', what)
        dfs.append(pp_utils.iamc_it(retr_cumulative_extraction(
            ix_mdl_nm[model], ix_scn_nm[scenario], pp, pp_utils), what, xlsx))

    # Primary Energy
    if primary_energy:
        what = 'Primary Energy'
        print('processing Table:', what)
        dfs.append(pp_utils.iamc_it(
            retr_pe(ix_mdl_nm[model], ix_scn_nm[scenario], pp, pp_utils), what, xlsx))

    # Final Energy
    if final_energy:
        what = 'Final Energy'
        print('processing Table:', what)
        dfs.append(pp_utils.iamc_it(
            retr_fe(ix_mdl_nm[model], ix_scn_nm[scenario], pp, pp_utils), what, xlsx))

    # Secondary Energy - Electricity Generation
    if SE_electricity_generation:
        what = 'Secondary Energy|Electricity'
        print('processing Table:', what)
        dfs.append(pp_utils.iamc_it(retr_SE_elecgen(
            ix_mdl_nm[model], ix_scn_nm[scenario], pp, pp_utils), what, xlsx))

    # Secondary Energy - District Heat
    if SE_district_heat:
        what = 'Secondary Energy|Heat'
        print('processing Table:', what)
        dfs.append(pp_utils.iamc_it(retr_SE_district_heat(
            ix_mdl_nm[model], ix_scn_nm[scenario], pp, pp_utils), what, xlsx))

    # Secondary Energy - Synthetic fuels
    if SE_synfuels:
        what = 'Secondary Energy'
        print('processing Table:', what, '|Liquids and |Hydrogen')
        dfs.append(pp_utils.iamc_it(retr_SE_synfuels(
            ix_mdl_nm[model], ix_scn_nm[scenario], pp, pp_utils), what, xlsx))

    # Secondary Energy - Gases
    if SE_gases:
        what = 'Secondary Energy|Gases'
        print('processing Table:', what)
        dfs.append(pp_utils.iamc_it(retr_SE_gases(
            ix_mdl_nm[model], ix_scn_nm[scenario], pp, pp_utils), what, xlsx))

    # Secondary Energy - Solids
    if SE_solids:
        what = 'Secondary Energy|Solids'
        print('processing Table:', what)
        dfs.append(pp_utils.iamc_it(retr_SE_solids(
            ix_mdl_nm[model], ix_scn_nm[scenario], pp, pp_utils), what, xlsx))

    # CO2 Emissions
    if CO2_emissions:
        what = 'Emissions|CO2'
        print('processing Table:', what)
        dfs.append(pp_utils.iamc_it(retr_CO2emi(
            ix_mdl_nm[model], ix_scn_nm[scenario], pp, pp_utils), what, xlsx))

    # CO2 CCS
    if CO2_CCS:
        what = 'Carbon Sequestration'
        print('processing Table:', what)
        dfs.append(pp_utils.iamc_it(retr_CO2_CCS(
            ix_mdl_nm[model], ix_scn_nm[scenario], pp, pp_utils), what, xlsx))

    # All other Emissions except F-Gases
    if other_emissions:
        for var in['BC', 'OC', 'CO', 'N2O', 'CH4', 'NH3', 'Sulfur', 'NOx', 'VOC']:
            what = 'Emissions|%s' % var
            print('processing Table:', what)
            dfs.append(pp_utils.iamc_it(retr_othemi(
                var, ix_mdl_nm[model], ix_scn_nm[scenario], pp, pp_utils), what, xlsx))

    # Total installed capacity
    if total_installed_capacity:
        what = 'Capacity'
        print('processing Table:', what)
        dfs.append(pp_utils.iamc_it(retr_ppl_capparameters(
            pp.tic, ix_mdl_nm[model], ix_scn_nm[scenario], pp, pp_utils), what, xlsx))

    # New installed capacity
    if new_installed_capacity:
        what = 'Capacity Additions'
        print('processing Table:', what)
        dfs.append(pp_utils.iamc_it(retr_ppl_capparameters(
            pp.nic, ix_mdl_nm[model], ix_scn_nm[scenario], pp, pp_utils), what, xlsx))

    # New installed capacity
    if cumulative_installed_capacity:
        what = 'Cumulative Capacity'
        print('processing Table:', what)
        dfs.append(pp_utils.iamc_it(retr_ppl_capparameters(
            pp.cumcap, ix_mdl_nm[model], ix_scn_nm[scenario], pp, pp_utils), what, xlsx))

    # Capital cost
    if capital_cost:
        what = 'Capital Cost'
        print('processing Table:', what)
        dfs.append(pp_utils.iamc_it(retr_ppl_parameters(
            pp.inv_cost, 'US$2010/kW', ix_mdl_nm[model], ix_scn_nm[scenario], pp, pp_utils), what, xlsx))

    # OM cost-Fixed
    if fix_cost:
        what = 'OM Cost|Fixed'
        print('processing Table:', what)
        dfs.append(pp_utils.iamc_it(retr_ppl_opcost_parameters(
            pp.fom, 'US$2010/kW/yr', ix_mdl_nm[model], ix_scn_nm[scenario], pp, pp_utils), what, xlsx))

    # OM cost-Variable
    if var_cost:
        what = 'OM Cost|Variable'
        print('processing Table:', what)
        dfs.append(pp_utils.iamc_it(retr_ppl_opcost_parameters(
            pp.vom, 'US$2010/kWh', ix_mdl_nm[model], ix_scn_nm[scenario], pp, pp_utils), what, xlsx))

    # Technical lifetime
    if tech_lifetime:
        what = 'Lifetime'
        print('processing Table:', what)
        dfs.append(pp_utils.iamc_it(retr_ppl_parameters(
            pp.pll, 'years', ix_mdl_nm[model], ix_scn_nm[scenario], pp, pp_utils), what, xlsx))

    # Efficiency
    if efficiency:
        what = 'Efficiency'
        print('processing Table:', what)
        dfs.append(pp_utils.iamc_it(retr_eff_parameters(
            ix_mdl_nm[model], ix_scn_nm[scenario], pp, pp_utils), what, xlsx))

    # Population
    if population:
        what = 'Population'
        print('processing Table:', what)
        dfs.append(pp_utils.iamc_it(retr_pop(
            ix_mdl_nm[model], ix_scn_nm[scenario], pp, pp_utils), what, xlsx))

    # Prices (energy and carbon)
    if prices:
        what = 'Price'
        print('processing Table:', what)
        dfs.append(pp_utils.iamc_it(retr_price(
            ix_mdl_nm[model], ix_scn_nm[scenario], pp, pp_utils), what, xlsx))

    # Demands useful
    if demands:
        what = 'Useful Energy'
        print('processing Table:', what)
        dfs.append(pp_utils.iamc_it(retr_demands_input(
            ix_mdl_nm[model], ix_scn_nm[scenario], pp, pp_utils), what, xlsx))

    # Demands useful
    if demands:
        what = 'Useful Energy'
        print('processing Table:', what)
        dfs.append(pp_utils.iamc_it(retr_demands_output(
            ix_mdl_nm[model], ix_scn_nm[scenario], pp, pp_utils), what, xlsx))

    # Supply Side Investements
    if supply_side_investments:
        what = 'Investment|Energy Supply'
        print('processing Table:', what)
        dfs.append(pp_utils.iamc_it(retr_supply_inv(
            ix_mdl_nm[model], ix_scn_nm[scenario], pp, pp_utils), what, xlsx))

    df = pd.concat(dfs)
    # Load reporting template and delete any variables from the dataframe
    # that are not in the template
    #ixmp_pth = ixmp.__path__
    #filin = os.path.join(ixmp_pth[0], 'iamc_template', xlsx)
#    filin = os.path.join(postprocess_path, 'iamc_template', xlsx)
#    tmpl_vars = pd.read_excel(
#        filin, sheet_name='variable_definitions', index_col=0)
#    select = df.Variable.isin(tmpl_vars.Variable.unique())
#    df = df.loc[select]

    if merge_hist:
        ix_upload = df.reset_index()
        ix_upload = ix_upload.drop(['index', 'Model', 'Scenario'], axis=1)
        ix_upload = ix_upload.rename(columns={
            'Region': 'region',
            'Variable': 'variable',
            'Unit': 'unit',
        })
        col_yr = pp_utils.numcols(df)
        model_year = int(
            ds.set("cat_year", {'type_year': ['firstmodelyear']})['year'])
        ix_regions = {
            'India': 'India',
        }
        ix_upload.region = ix_upload.region.map(ix_regions)
        if ref_sol == 'True':
            cols = ['region', 'variable', 'unit'] + [int(yr) for yr in col_yr]
        else:
            cols = ['region', 'variable', 'unit'] + \
                [int(yr) for yr in col_yr if yr >= model_year]
        ix_upload = ix_upload[cols]
        # ix_mp._jobj.unlockRunid(114)
        ds.check_out(timeseries_only=True)
        ds.add_timeseries(ix_upload)
        ds.commit('Reporting uploaded as timeseries')

#        df = ds.timeseries(IAMC=True)
#        # df = df.pivot_table(
#        #       'value', ['node', 'key', 'unit'], 'year').reset_index()
#        df = df.rename(columns={
#            'node': 'Region',
#            'key': 'Variable',
#            'unit': 'Unit',
#        })
#        df['Model'] = ix_mdl_nm[model]
#        df['Scenario'] = ix_scn_nm[scenario]
#        regions = {
#            'India': 'India',
#        }
#        df.Region = df.Region.map(regions)
#        df = df.set_index(['Model', 'Scenario', 'Region',
#                           'Variable', 'Unit']).reset_index()

    #out_dir = out_dir or default_paths.UPLOAD_DIR
    out_dir = os.path.join(postprocess_path, 'output')
    pp_utils.write_xlsx(df, ix_mdl_nm[model], ix_scn_nm[scenario], out_dir)

    if db_upload == True:
        os.chdir(default_paths.UPLOAD_DIR)
        fil_out = '{}_{}.xlsx'.format(
            ix_mdl_nm[model], ix_scn_nm[scenario])
        user = 'fricko'
        os.system('python CfgExcelDbImport.py %s %s' % (fil_out, user))

        os.chdir(default_paths.REPORTING_DIR)


def read_args():
    descr = """
    IAMC reporting for the IX-Platform

    Example usage:
    python iamc_report.py input.xlsx history.csv --scenario [ix-scenario name]

    """
    parser = argparse.ArgumentParser(description=descr,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    scenario = '--scenario : string\n    ix-scenario name'
    parser.add_argument('--scenario', help=scenario)
    scenario_out = '--scenario_out : string\n    iamc scenario output name'
    parser.add_argument('--scenario_out', help=scenario_out)
    model = '--model : string\n    ix-model name'
    parser.add_argument('--model', help=model)
    model_out = '--model_out : string\n    iamc model output name'
    parser.add_argument('--model_out', help=model_out)
    version = '--version : integer\n    ix-run version number'
    parser.add_argument('--version', help=version)
    ref_sol = '--ref_sol : string (standard=False)\n    True for non ix solution\n    False for ix-solution'
    parser.add_argument('--ref_sol', help=ref_sol)
    db_upload = '--db_upload : string (standard=True)\n    True uploads results into db\n    False does not upload results into db'
    parser.add_argument('--db_upload', help=db_upload, action="store_true")
    merge_hist = '--merge_hist : string (standard=True)\n    True merges pre 2020 values into db\n    False does not merge pre 2020 values'
    parser.add_argument('--merge_hist', help=merge_hist, action="store_true")
    out_dir = 'output directory'
    parser.add_argument('--out_dir', help=out_dir)

    # parse cli
    args = parser.parse_args()
    return args


def main():
    args = read_args()
    scenario = args.scenario or None
    scenario_out = args.scenario_out or None
    model = args.model or None
    model_out = args.model_out or None
    ref_sol = args.ref_sol or None
    version = int(args.version) if args.version else None
    merge_hist = args.merge_hist
    db_upload = args.db_upload
    out_dir = args.out_dir

    ix_scn_nm = {}
    ix_mdl_nm = {}
    if not ref_sol:
        ref_sol = 'False'
        if scenario:
            if scenario_out:
                ix_scn_nm[scenario] = scenario_out
            else:
                ix_scn_nm[scenario] = scenario
        else:
            ix_scn_nm = {
                #'baseline': 'INDC_baseline',
                #'baseline_fixed_EXT': 'SSP2_Ref_baseline_fixed_EXT',
                #'baseline_fixed_EXT_no_vtg': 'SSP2_Ref_baseline_fixed_EXT_no_vtg',
                #'baseline_no_vtg': 'SSP2_Ref_baseline_no_vtg',
                #'vintage_test': 'SSP2_Ref_vintage_test',
                #'ssp2NPi2020-con-prim-dir-ncr': 'SSP2_NPi2020-con-prim-dir-ncr',
                #'refinery_renorm': 'SSP2_Ref_SPA0_refinery_renorm',
                #'ssp2NPi2020-con-prim-dir-ncr': 'ssp2NPi2020-con-prim-dir-ncr_SHR1'
                #'emission bound test': 'emission_bound_test_1'
                'baseline': 'SSP2_INDC_baseline',
                'NPi2020-con-prim-dir-ncr': 'SSP2_INDC_NPi2020-con-prim-dir-ncr',
                'NPi2030-con-prim-dir-ncr': 'SSP2_INDC_NPi2030-con-prim-dir-ncr',
                'NPiREF-con-prim-dir-ncr': 'SSP2_INDC_NPiREF-con-prim-dir-ncr',
                'NPi2020_1600-con-prim-dir-ncr': 'SSP2_INDC_NPi2020_1600-con-prim-dir-ncr',
                'NPi2020_1000-con-prim-dir-ncr': 'SSP2_INDC_NPi2020_1000-con-prim-dir-ncr',
                'NPi2020_400-con-prim-dir-ncr': 'SSP2_INDC_NPi2020_400-con-prim-dir-ncr',
                'INDC2030i-con-prim-dir-ncr': 'SSP2_INDC_INDC2030i-con-prim-dir-ncr',
                'INDC2030i_forever-con-prim-dir-ncr': 'SSP2_INDC_INDC2030i_forever-con-prim-dir-ncr',
                'INDCi_1600-con-prim-dir-ncr': 'SSP2_INDC_INDCi_1600-con-prim-dir-ncr',
                'INDCi_1000-con-prim-dir-ncr': 'SSP2_INDC_INDCi_1000-con-prim-dir-ncr',
                'NPip2020-con-prim-dir-ncr': 'SSP2_INDC_NPip2020-con-prim-dir-ncr',
                'NPip2030-con-prim-dir-ncr': 'SSP2_INDC_NPip2030-con-prim-dir-ncr',
            }
        if model:
            if model_out:
                ix_mdl_nm[model] = model_out
            else:
                ix_mdl_nm[model] = model
        else:
            # ix_mdl_nm = {'MESSAGE-GLOBIOM SSP2': 'MESSAGEix_test'}
            # ix_mdl_nm = {'MESSAGE-GLOBIOM CD-LINKS R2': 'MESSAGEix_test'}
            # ix_mdl_nm = {'MESSAGE-GLOBIOM CD-LINKS R2':
            # 'SSP_INDC_CD-Links_R2'}
            ix_mdl_nm = {
                'MESSAGE-GLOBIOM CD-LINKS R2.1': 'SSP_INDC_CD-Links_R2.1'}
    else:
        ref_sol == 'True'
        if scenario:
            if scenario_out:
                ix_scn_nm[scenario] = scenario_out
            else:
                ix_scn_nm[scenario] = scenario
        else:
            # ix_scn_nm = {'baseline': 'SSP2_Ref_SPA0_rf'}
            # ix_scn_nm = {'baseline': 'MESSAGEix-GLOBIOM_SSP2_rf'}
            ix_scn_nm = {'baseline': 'SSP2_INDC_baseline_rf'}
        if model:
            if model_out:
                ix_mdl_nm[model] = model_out
            else:
                ix_mdl_nm[model] = model
        else:
            # ix_mdl_nm = {'MESSAGE-GLOBIOM SSP2': 'MESSAGEix_test'}
            # ix_mdl_nm = {'MESSAGE-GLOBIOM CD-LINKS R2': 'MESSAGEix_test'}
            # ix_mdl_nm = {'MESSAGE-GLOBIOM CD-LINKS R2':
            # 'SSP_INDC_CD-Links_R2'}
            ix_mdl_nm = {
                'MESSAGE-GLOBIOM CD-LINKS R2.1': 'SSP_INDC_CD-Links_R2.1'}

#    version=1

    ix_mp = ixmp.Platform()
    for model in ix_mdl_nm.keys():
        for scenario in ix_scn_nm.keys():
            ds = ix_mp.Scenario(
                model, scenario, version=version, cache=True)
            # run
            report(ix_mp, ds, ref_sol, ix_mdl_nm, ix_scn_nm, model, scenario,
                   version, db_upload, merge_hist, out_dir)


if __name__ == '__main__':
    main()
