# Energy Model and Simulation Tool
An open-source ground level energy modelling and wide application of scientific approach of energy planning at any level. For this repository, we will refer to the West Bengal energy data

## About the Decision Support Tool

The Tool is developed using Flask framework of python. This unique tool can help the users to simulate any energy policy scenario using MESSAGEix ( IIASA's open source energy modelling platform) without having any prior modelling knowledge. The tool is developed mainly to bring the systems modelling to the policy makers’ level who are not interested in modelling complexity but more on results. Our idea behind it is to bring the scientific knowledge and information behind grass root level policy making activities especially in the developing countries. For that simplicity of modelling is essential. This tool can run on tab / mobile and simulation results can be instantaneously displayed. There are more than 50 indicators which are captured in the tool for policy makers to look into.

## Resource Required

Download IXMP from here - https://github.com/iiasa/ixmp
Download MESSAGEix from here - https://github.com/iiasa/message_ix

Following Python libraries are required:
  Numpy
  Pandas
  itertools

xlsx_core.py handles all the Excel related operations.

The baseline file is iamc_report_india.py - from iamc_report_india import report as reporting

### Retrieve MESSAGE_DATA_PATH
msg_data_path = os.environ['MESSAGE_DATA_PATH']
postprocess_path = '{}/post-processing/reporting/'.format(msg_data_path)
print postprocess_path
if postprocess_path not in sys.path:
    sys.path.append(postprocess_path)

## License

Copyright © 2018–2019 IIASA Energy Program

The MESSAGEix framework is licensed under the Apache License, Version 2.0 (the "License"); you may not use the files in this repository except in compliance with the License. You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0.

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.

Please refer to the NOTICE for details and user guidelines.
