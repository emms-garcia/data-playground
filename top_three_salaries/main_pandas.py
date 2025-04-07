import pandas as pd


def run():
    # 1) Read the employees and departments dataframes
    employees_df = pd.read_csv("data/employees.csv")
    employees_df = employees_df.rename(columns={
        "id": "employee_id",
        "name": "employee",
    })
    departments_df = pd.read_csv("data/departments.csv")
    departments_df = departments_df.rename(columns={
        "id": "department_id",
        "name": "department",
    })
    # 2) Merge the dataframes on the department_id
    df = employees_df.merge(departments_df, on="department_id", how="inner")
    # 3) Group the dataframe by department_id and assign ranks based on the salary
    df["rank"] = df.groupby("department_id")["salary"].rank(method="dense", ascending=False)
    # 4) Keep only the Top 3 salaries by department
    df = df[df["rank"] <= 3]
    df = df[["department", "employee", "salary"]]

    expected_df = pd.read_csv("data/result.csv")
    pd.testing.assert_frame_equal(expected_df.reset_index(drop=True), df.reset_index(drop=True))
    print("Success!")


if __name__ == "__main__":
    run()
