import asyncio
import logging

import aiohttp
import pandas as pd
from google.cloud import bigquery

logger = logging.getLogger(__name__)

query = """
  SELECT
    u.user_id
  , credit_score
  , 0 AS acv
  , selected_packages
  , zip_code
  , state
  , DATE_DIFF(CURRENT_DATE(), DATE(u.date_of_birth), YEAR) AS age
  , gender
  , CASE
        WHEN selected_packages = 'minimum-liability-az'
            THEN '25/50'
        WHEN selected_packages = 'full-cover-az'
            THEN '25/50'
        WHEN selected_packages = 'regular-coverage-az'
            THEN '50/100'
    END               AS bi
  , CASE
        WHEN selected_packages = 'minimum-liability-az'
            THEN '15000'
        WHEN selected_packages = 'full-cover-az'
            THEN '15000'
        WHEN selected_packages = 'regular-coverage-az'
            THEN '50000'
    END               AS pd
  , CASE
        WHEN selected_packages = 'minimum-liability-az'
            THEN 'None'
        WHEN selected_packages = 'full-cover-az'
            THEN '500'
        WHEN selected_packages = 'regular-coverage-az'
            THEN '500'
    END               AS comp
  , CASE
        WHEN selected_packages = 'minimum-liability-az'
            THEN 'None'
        WHEN selected_packages = 'full-cover-az'
            THEN '500'
        WHEN selected_packages = 'regular-coverage-az'
            THEN '500'
    END               AS coll
  , dpv.make
  , dpv.model
  , dpv.year
  , dpv.vin
  , policy_active
    FROM
        `data-warehouse-267920`.just_dim_warehouse.dim_user u
            INNER JOIN `data-warehouse-267920`.just_dim_warehouse.dim_policy_vehicle dpv
                       ON u.user_id = dpv.user_id
    WHERE
          policy_active IS NOT NULL
      AND date_of_birth IS NOT NULL
      AND selected_packages IS NOT NULL
      AND credit_score IS NOT NULL
      AND (
            date(u.update_date) > current_date() - 1 
            OR
            u.user_id not in (select user_id from `price_comparison.competitor_prices_by_user`))"""


async def run_main():
    client = bigquery.Client("data-warehouse-267920")
    query_job = client.query(query)
    df = query_job.to_dataframe()

    # url = "http://127.0.0.1:8000/compare"
    url = "https://comparator-axjyerdcyq-uc.a.run.app/compare"

    difference_list = []
    exception_list = []
    responses = []

    # calling the api
    async def fetch_comparison(session, body, url, row_index, user_id):
        try:
            async with session.post(url, json=body, timeout=360) as response:
                try:
                    response_json = await response.json()
                    response_json['index'] = row_index
                    response_json['user_id'] = user_id
                    responses.append(response_json)
                except TimeoutError as e:
                    exception_list.append((row_index, e))
                    # logger.error(f"Error for index {row_index}: {e}")
                except Exception as e:
                    try:
                        async with session.post(url, json=body, timeout=360) as response_2:
                            response_text = await response_2.text()
                    except TimeoutError as e:
                        exception_list.append((row_index, e))
                    except Exception as e:
                        exception_list.append((row_index, e))
        except TimeoutError as e:
            exception_list.append((row_index, e))
            # logger.error(f"Error for index {row_index}: {e}")
        except Exception as e:
            exception_list.append((row_index, e))

    # modifying data to fit api requirements
    async def run_sample():
        # for row in df.sample(10**4).iterrows():
        async with aiohttp.ClientSession() as session:
            tasks = []
            for row in df.iterrows():
                row = row[1]
                # partial vin from vin with wildcard for check (1gykpdrs.r)
                partial_vin = row["vin"][:8].upper() + '.' + row["vin"][9].upper()

                # insurance_score
                # 800
                # 525
                # 675
                # 400

                if row['credit_score'] >= 800:
                    row['credit_score'] = 800
                elif row['credit_score'] >= 675:
                    row['credit_score'] = 675
                elif row['credit_score'] >= 525:
                    row['credit_score'] = 525
                else:
                    row['credit_score'] = 400

                payload = {
                    "state": "AZ",
                    "age": row["age"],
                    "gender": row["gender"].lower(),
                    # "marital_status": row[""].lower(), # default to single
                    "acv": row["acv"],
                    "zip": str(row["zip_code"]),
                    "insurance_score": str(row["credit_score"]),
                    "pd_limit": str(row["pd"]),
                    "bi_limit": str(row["bi"]),
                    "comp_deductible": str(row['comp']),
                    "coll_deductible": str(row["coll"]),
                    "vehicle": {
                        "make": row["make"],
                        "model": row["model"],
                        "year": row["year"],
                        "partial_vin": partial_vin
                    }
                }
                tasks.append(fetch_comparison(session, payload, url, row.name, row['user_id']))
                # print(payload)
            await asyncio.gather(*tasks)

    await run_sample()

    # appending new api data to base data and recording any errors
    # difference_list = pd.DataFrame([ {'index': r['index'], **rates} for r in responses for rates in r.get('rates'] ])
    competitor_prices_list = []
    errors = []

    print("Number of Responses: ", len(responses))

    print(f"Number of Users that encountered error in request:  {len(exception_list)}")
    from collections import Counter
    
    # Count exceptions by type
    exceptions_count = Counter(type(exc[1]).__name__ for exc in exception_list)
    
    # exceptions_count
    print(exceptions_count)

    for r in responses:
        try:
            for rates in r['rates']:
                rates['index'] = r['index']
                rates['upper_rate'] = rates['daily_rate']['upper_rate']
                rates['lower_rate'] = rates['daily_rate']['lower_rate']
                rates['rate'] = rates['daily_rate']['rate']
                competitor_prices_list.append(rates)
        except KeyError as e:
            # Something Wrong With Data Validation in Comparison API
            errors.append(
                {'msg': r['detail'][0]['msg']}
            )
            continue

    # putting new data and error data into dataframes

    competitor_prices_list = pd.DataFrame(competitor_prices_list)
    df_errors = pd.DataFrame(errors)

    competitor_prices_list.set_index('index', inplace=True)

    competitor_prices = competitor_prices_list[['carrier', 'rate', 'upper_rate', 'lower_rate']].rename(
        columns={'rate': 'estimate'})
    competitor_prices = competitor_prices.join(df)

    job_config = bigquery.LoadJobConfig(
        autodetect=True,
        write_disposition="WRITE_APPEND",
        create_disposition="CREATE_IF_NEEDED",
        schema_update_options="ALLOW_FIELD_ADDITION",
    )

    # TODO: Change the table id to the correct one
    TABLE_ID = "price_comparison.competitor_prices_by_user"

    client = bigquery.Client("data-warehouse-267920")
    job = client.load_table_from_dataframe(
        competitor_prices,
        destination=TABLE_ID,
        job_config=job_config
    )

    job.result()

    return True


def main(request):
    asyncio.run(run_main())
    return "200 OK"


if __name__ == '__main__':
    asyncio.run(run_main())
