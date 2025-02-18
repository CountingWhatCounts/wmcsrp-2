{% docs __overview__ %}
# WMCSRP 2.0



## Introduction

This documentation summarises the data which has been brought together for the West Midlands Cultural Sector Research Project 2.0 and the process used to do so.

To produce the dataset on your own computer, you can follow the instructiong on the GitHub page here: https://github.com/CountingWhatCounts/wmcsrp-2. 



## How to use this website

By clicking on the **Database** button on the left hand side you can view the content of the database.

The tables are grouped into 4 schemas
1. **public**: contains the preprocessed source data
2. **public_staging**: contains initial data models
3. **public_warehouse**: contains intermediate data models
4. **public_marts**: contains the final data models

The data used for the project is in the final data models in public_marts. If you click on any of the tables in the public_marts schema you can then see documentation about the source and contents of that table, including:
* The source of the data
* The columns present the table with a description of their content

In the bottom right-hand side of the page there is a **View Lineage Graph** button which shows you the flow of data from the raw data to the marts. If you click on a table in this graph you can see how other tables were combined to produce this output.



## Data Sources
| Data Source                                 | Data Theme              | Link                                                                                                                                       |
| :------------------------------------------ | :---------------------- | :----------------------------------------------------------------------------------------------------------------------------------------- |
| Arts Council England NPO Funding            | Funding                 | https://www.artscouncil.org.uk/how-we-invest-public-money/2023-26-Investment-Programme/2023-26-investment-programme-data                   |
| Arts Council England Project Grants Funding | Funding                 | https://www.artscouncil.org.uk/ProjectGrants/project-grants-data                                                                           |
| Grant360                                    | Funding                 | https://grantnav.threesixtygiving.org/                                                                                                     |
| Census 2021                                 | Demographics            | https://www.nomisweb.co.uk/sources/census_2021_bulk                                                                                        |
| Annual Survey Economic Data                 | Economic                | https://www.nomisweb.co.uk/query/construct/summary.asp?mode=construct&version=0&dataset=17                                                 |
| Annual Personal Wellbeing Estimates         | Wellbeing               | https://www.ons.gov.uk/peoplepopulationandcommunity/wellbeing/datasets/headlineestimatesofpersonalwellbeing                                |
| Cultural and Place Data Explorer            | Cultural Infrastructure | https://culture.localinsight.org/#/map                                                                                                     |
| Indices of Multiple Deprivation             | Place Characteristics   | https://www.nomisweb.co.uk/query/construct/summary.asp?mode=construct&version=0&dataset=17                                                 |
| Rural Urban Classification                  | Place Characteristics   | https://geoportal.statistics.gov.uk/datasets/ons::rural-urban-classification-2011-of-msoas-in-ew/about                                     |
| Levelling Up and Priority Places            | Place Characteristics   | https://www.artscouncil.org.uk/your-area/priority-places-and-levelling-culture-places#t-in-page-nav-3                                      |
| ONS Postcode Directory (November 2024)      | Area Mapping            | https://geoportal.statistics.gov.uk/datasets/b54177d3d7264cd6ad89e74dd9c1391d/about                                                        |
| MSOA to Local Authority District Mapping    | Area Mapping            | https://geoportal.statistics.gov.uk/datasets/ons::msoa-2011-to-msoa-2021-to-local-authority-district-2022-exact-fit-lookup-for-ew-v2/about |



## West Midlands areas and codes
There are 30 lower tier local authority areas and 14 upper tier local authority areas in the West Midlands area, with 7 of them together forming the West Midlands Combined Authority. There are 736 MSOAs in total across these local authorities.


| #   | Lower Tier               | Upper Tier               | MSOA Count | In WMCA? |
| --- | ------------------------ | ------------------------ | ---------- | -------- |
| 1   | Birmingham               | Birmingham               | 132        | Yes      |
| 2   | Coventry                 | Coventry                 | 42         | Yes      |
| 3   | Dudley                   | Dudley                   | 43         | Yes      |
| 4   | Herefordshire, County of | Herefordshire, County of | 23         |          |
| 5   | Sandwell                 | Sandwell                 | 39         | Yes      |
| 6   | Shropshire               | Shropshire               | 39         | Yes      |
| 7   | Solihull                 | Solihull                 | 29         |          |
| 8   | Stafford                 | Staffordshire            | 16         |          |
| 9   | Lichfield                | Staffordshire            | 12         |          |
| 10  | Tamworth                 | Staffordshire            | 10         |          |
| 11  | South Staffordshire      | Staffordshire            | 13         |          |
| 12  | Cannock Chase            | Staffordshire            | 13         |          |
| 13  | Staffordshire Moorlands  | Staffordshire            | 13         |          |
| 14  | East Staffordshire       | Staffordshire            | 15         |          |
| 15  | Newcastle-under-Lyme     | Staffordshire            | 16         |          |
| 16  | Stoke-on-Trent           | Stoke-on-Trent           | 33         |          |
| 17  | Telford and Wrekin       | Telford and Wrekin       | 24         |          |
| 18  | Walsall                  | Walsall                  | 39         | Yes      |
| 19  | Rugby                    | Warwickshire             | 13         |          |
| 20  | Stratford-on-Avon        | Warwickshire             | 15         |          |
| 21  | North Warwickshire       | Warwickshire             | 7          |          |
| 22  | Warwick                  | Warwickshire             | 15         |          |
| 23  | Nuneaton and Bedworth    | Warwickshire             | 17         |          |
| 24  | Wolverhampton            | Wolverhampton            | 33         | Yes      |
| 25  | Wychavon                 | Worcestershire           | 19         |          |
| 26  | Bromsgrove               | Worcestershire           | 14         |          |
| 27  | Malvern Hills            | Worcestershire           | 11         |          |
| 28  | Worcester                | Worcestershire           | 14         |          |
| 29  | Wyre Forest              | Worcestershire           | 14         |          |
| 30  | Redditch                 | Worcestershire           | 13         |          |

Each local authority and MSOA has both a name and a code. We have used the 2023 codes for the local authorities and the 2021 codes for the MSOAs.

If the data source used 2011 MSOA codes then these were mapped onto the 2021 codes using ONS lookup tables. If the data source used postcodes then similarly these were mapped onto 2021 MSOA codes using an ONS lookup table.


{% enddocs %}

