import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy import inspect
from sqlalchemy.sql import text
from sqlalchemy.exc import OperationalError
import matplotlib.pyplot as plt
import mpld3
import streamlit.components.v1 as components


def connect_to_db(your_username, your_password, your_host, your_database):
    engine = create_engine(f'postgresql://{your_username}:{your_password}@{your_host}/{your_database}')
    try:
        # Attempt to connect to the database
        connection = engine.connect()
        # Close the connection
        connection.close()
        return engine
    except OperationalError:
        return None


def fetch_data(query):
    query += " ORDER BY index"
    df = pd.read_sql(query, st.session_state.engine)
    # fill NaN values with empty string
    df.fillna("", inplace=True)
    return df


def fetch_table_names():
    st.session_state.engine.connect()
    insp = inspect(st.session_state.engine)
    table_names = insp.get_table_names()
    return table_names


def fetch_column_names():
    st.session_state.engine.connect()
    insp = inspect(st.session_state.engine)
    columns = insp.get_columns(st.session_state.table_name)
    columns = [column['name'] for column in columns]
    return columns


def fetch_column_data():
    st.session_state.engine.connect()
    insp = inspect(st.session_state.engine)
    columns = insp.get_columns(st.session_state.table_name)
    # create a dataframe to store the column data
    df_columns = pd.DataFrame(columns)
    # convert df_columns["type"] to string and then to category
    df_columns["type"] = df_columns["type"].astype(str)
    df_columns["type"] = df_columns["type"].astype("category").cat.add_categories(["..."])
    return df_columns


def update_database(index, column, new_value):
    query = text(f"UPDATE {st.session_state.table_name} SET \"{column}\" = '{new_value}' WHERE index = {index}")
    # run the query
    with st.session_state.engine.connect() as connection:
        # Execute the query with parameterized values
        connection.execute(query)
        # Commit the transaction
        connection.commit()


def update_column_data(index, column, new_value):
    if new_value is None:
        return
    column_name = fetch_column_names()[index]
    # check if the column is of type numeric
    if column == "type":
        query = f"ALTER TABLE {st.session_state.table_name} " \
                f"ALTER COLUMN \"{column_name}\" TYPE {new_value} USING \"{column_name}\"::{new_value}"
    elif column == "comment":
        query = f"COMMENT ON COLUMN \"{st.session_state.table_name}\".\"{column_name}\" IS '{new_value}'"
    elif column == "name":
        query = f"ALTER TABLE {st.session_state.table_name} RENAME COLUMN \"{column_name}\" TO \"{new_value}\""
    else:
        return
    # run the query
    with st.session_state.engine.connect() as connection:
        # Execute the query with parameterized values
        connection.execute(text(query))
        # Commit the transaction
        connection.commit()


def create_table(table_name, path):
    df = pd.read_excel(path)
    df.to_sql(table_name, st.session_state.engine, if_exists='replace', index=True)


def load_data(path):
    df = pd.read_excel(path)
    df.to_sql(st.session_state.table_name, st.session_state.engine, if_exists='append', index=False)


def export_to_excel():
    # Export the data to an Excel file
    st.session_state.df_value.to_excel("data.xlsx", index=False)


def compute_new_columns(column_name, calculation):
    query = text(
        f"AlTER TABLE {st.session_state.table_name} "
        f"ADD COLUMN \"{column_name}\" NUMERIC GENERATED ALWAYS AS ({calculation}) STORED")
    with st.session_state.engine.connect() as connection:
        connection.execute(query)
        connection.commit()


def add_row():
    existing_dataframe = fetch_data(f"SELECT * FROM {st.session_state.table_name}")
    # Create an empty DataFrame with the same columns
    empty_row = pd.DataFrame([[""] * len(existing_dataframe.columns)], columns=existing_dataframe.columns)
    # set index in the first column to last index in dataframe
    empty_row["index"] = existing_dataframe["index"].max() + 1
    # Concatenate the original DataFrame with the empty row DataFrame
    st.session_state.df_value = pd.concat([existing_dataframe, empty_row], ignore_index=True)
    # append empty row to the database
    st.session_state.df_value.to_sql(st.session_state.table_name,
                                     st.session_state.engine,
                                     if_exists='replace',
                                     index=False)


def delete_row(selected_rows):
    # get indexes of selected rows
    indexes = selected_rows.index.tolist()
    if not indexes:
        # write a message to the user for 5 seconds
        st.warning("Please select a row to delete.")
        return
    # drop the rows from the database
    for index in indexes[::-1]:
        query_delete = text(f"DELETE FROM {st.session_state.table_name} WHERE index = {index + 1}")
        query_update_index = text(f"UPDATE {st.session_state.table_name} "
                                  f"SET index = index - 1 WHERE index > {index + 1}")
        with st.session_state.engine.connect() as connection:
            connection.execute(query_delete)
            connection.execute(query_update_index)
            connection.commit()

    # drop from data editor
    st.session_state.df_value = st.session_state.df_value.drop(selected_rows.index[0], inplace=True)


def delete_column(column_to_delete):
    query = text(f"ALTER TABLE {st.session_state.table_name} DROP COLUMN \"{column_to_delete}\"")
    with st.session_state.engine.connect() as connection:
        connection.execute(query)
        connection.commit()
    st.session_state.df_value = st.session_state.df_value.drop(column_to_delete, axis=1, inplace=True)


def delete_table():
    query = text(f"DROP TABLE {st.session_state.table_name}")
    with st.session_state.engin.connect() as connection:
        connection.execute(query)
        connection.commit()


# Streamlit UI
def main():
    st.title('Interface for PostgreSQL Database')

    # Initialize session state variables if they don't exist
    if "table_name" not in st.session_state:
        st.session_state.table_name = ""
    if "df_value" not in st.session_state:
        st.session_state.df_value = None
    if "table_names" not in st.session_state:
        st.session_state.table_names = None
    if "table_name" not in st.session_state:
        st.session_state.table_name = None
    if "query" not in st.session_state:
        st.session_state.query = ""
    if "column_name" not in st.session_state:
        st.session_state.column_name = ""
    if "columns_meta" not in st.session_state:
        st.session_state.columns_meta = None
    if "engine" not in st.session_state:
        st.session_state.engine = None

    if st.session_state.engine is None:
        form_expanded = True
    else:
        form_expanded = False

    with st.expander("Create Connection to Database", expanded=form_expanded):
        with st.form(key='create_connection_form'):
            your_username = st.text_input("Enter Username")
            your_password = st.text_input("Enter Password", type="password")
            your_host = st.text_input("Enter Host")
            your_database = st.text_input("Enter Database")

            if st.form_submit_button("Connect to Database"):
                st.session_state.engine = connect_to_db(your_username, your_password, your_host, your_database)
                if st.session_state.engine is None:
                    st.write("Connection failed. Engine is not valid.")
                else:
                    st.rerun()

    if st.session_state.engine is None:
        return

    st.write("Connected to PostgreSQL Database")

    st.session_state.table_names = fetch_table_names()

    st.session_state.table_name = st.selectbox("Select Table", st.session_state.table_names)

    #  Create a sidebar
    st.sidebar.title('Menu')
    with st.sidebar.expander('Create/Delete Table'):
        table_name_input = st.text_input("Enter Table Name")
        # Use form submission to create a new table
        with st.form(key='create_new_table_form'):
            # create drag and drop file uploader
            uploaded_file = st.file_uploader("Upload a Excel file", type="xlsx")
            if st.form_submit_button("Create Table") and table_name_input and uploaded_file:
                create_table(table_name_input, uploaded_file)
                st.session_state.table_name = table_name_input
                # Update the table names in the select box
                st.session_state.table_names.append(table_name_input)
                st.session_state.table_name = table_name_input
                # the selection to select the newly created table
                st.write(f"Table {table_name_input} created.")
            else:
                st.write("Please enter the table name and upload a file to create a new table")
        with st.form(key='delete_table_form'):
            if st.form_submit_button("Delete Table"):
                if table_name_input:
                    delete_table()
                    st.write(f"Table {st.session_state.table_name} was deleted.")
                    st.session_state.table_name = None
                else:
                    st.write("Please enter the table name to delete.")

    with st.sidebar.expander("Load Data", expanded=False):
        # Use form submission to create a new table
        with st.form(key='load_data_form'):
            # create drag and drop file uploader
            uploaded_file = st.file_uploader("Upload a Excel file", type="xlsx")
            if st.form_submit_button("Load Data") and uploaded_file:
                load_data(uploaded_file)
                st.write(f"Data loaded into table {st.session_state.table_name}")
            else:
                st.write("Please upload a file to load data into the table")

        # create an expander to display the table schema
        with st.sidebar.expander("Table Schema", expanded=False):
            if st.session_state.columns_meta is None:
                if st.session_state.table_name is None:
                    return
                st.session_state.columns_meta = fetch_column_data()

            st.data_editor(st.session_state.columns_meta, height=200, key="columns_editor")

            if st.session_state["columns_editor"]["edited_rows"] or st.session_state["columns_editor"][
                "added_rows"] or \
                    st.session_state["columns_editor"]["deleted_rows"]:
                for index, value in st.session_state["columns_editor"]["edited_rows"].items():
                    column = list(value.keys())[0]
                    if column == "Select":
                        continue
                    new_value = value[column]
                    update_column_data(index, column, new_value)

        # create an expander to compute new columns
        with st.sidebar.expander("Compute Columns", expanded=False):
            # create a form to calculate new columns
            def callback():
                st.session_state.calculation += f" \"{st.session_state.column}\""

            st.selectbox("Select Column", fetch_column_names(), on_change=callback,
                         key="column")
            new_column_name = st.text_input("Enter New Column Name")
            calculation = st.text_input("Enter Calculation", key="calculation")

            if st.button("Calculate New Column"):
                compute_new_columns(new_column_name, calculation)
                st.write(f"New column {new_column_name} calculated.")

    # create an expander to query the database
    with st.expander("Query Database", expanded=True):
        # Create a list to store column names
        columns = fetch_column_names()

        # Initialize session state variables
        if 'num_rows' not in st.session_state:
            st.session_state.num_rows = 1

        # Iterate over the number of rows
        for i in range(st.session_state.num_rows):
            query_col0, query_col1, query_col2, query_col3, query_col4 = st.columns([1, 1, 1, 1, 1])
            with query_col0:
                if i > 0:
                    st.selectbox("", ["AND", "OR"], key=f"chaining_{i + 1}")
            with query_col1:
                st.selectbox(f"Column {i + 1}", columns, key=f"column_{i + 1}")
            with query_col2:
                st.selectbox(f"Comparison {i + 1}", ["=", ">", "<", ">=", "<=", "!="], key=f"comparison_{i + 1}")
            with query_col3:
                st.text_input(f"Value {i + 1}", key=f"value_{i + 1}")
            with query_col4:
                if i == 0:  # First row always has "add" option
                    if st.button("add", key=f"add_row_{i + 1}"):
                        st.session_state.num_rows += 1
                        # update view
                        st.rerun()
                else:  # For subsequent rows, show "delete" option
                    if st.button("delete", key=f"delete_row_{i + 1}"):
                        st.session_state.num_rows -= 1
                        # update view
                        st.rerun()
        _, _, col1, col2 = st.columns([1, 1, 1, 1])
        with col1:
            if st.button("Run query"):
                # find all the query parameters
                query = f"Select * FROM {st.session_state.table_name} WHERE "
                # iterate over the rows in the expander
                for i in range(st.session_state.num_rows):
                    column = st.session_state[f"column_{i + 1}"]
                    comparison = st.session_state[f"comparison_{i + 1}"]
                    value = st.session_state[f"value_{i + 1}"]
                    chaining = st.session_state[f"chaining_{i + 1}"] if i > 0 else ""
                    query += f" {chaining} \"{column}\" {comparison} '{value}'"
                st.session_state.query = query
        with col2:
            if st.button("Clear query"):
                # Reset query
                st.session_state.query = ""
                st.session_state.num_rows = 1
                # update view
                st.rerun()

    # check if the selected table is different from the one in the session state
    if st.session_state.table_name is None:
        return

    # Fetch data from the database
    df = fetch_data(st.session_state.query if st.session_state.query
                    else f"SELECT * FROM {st.session_state.table_name}")
    st.session_state.df_value = df

    # Display the data in a table using Streamlit
    st.write(f"Data from PostgreSQL Database Table {st.session_state.table_name}:")
    # disable index column
    column_config = {1: st.column_config.Column(disabled=True)}
    df_with_selections = st.session_state.df_value.copy()
    df_with_selections.insert(1, "Select", False)
    edited_df = st.data_editor(df_with_selections, key="data_editor", hide_index=True, column_config=column_config, )

    # check if st.session_state["data_editor"] is not empty
    if st.session_state["data_editor"]["edited_rows"] or st.session_state["data_editor"]["added_rows"] or \
            st.session_state["data_editor"]["deleted_rows"]:
        for row, value in st.session_state["data_editor"]["edited_rows"].items():
            index = st.session_state.df_value["index"][row]
            column = list(value.keys())[0]
            if column == "Select":
                continue
            new_value = value[column]
            update_database(index, column, new_value)

    with st.sidebar.expander("Edit Data", expanded=False):
        # export to excel
        st.button("Export to Excel", on_click=export_to_excel)
        # add row
        st.button("Add row", on_click=add_row)
        # delete row
        st.button("Delete row", on_click=delete_row, args=(edited_df[edited_df.Select],))
        # delete column
        columns_to_delete = fetch_column_names()
        columns_to_delete.remove("index")
        # Open a modal dialog
        column_to_delete = st.selectbox('Select an column', columns_to_delete, key="column_to_delete")
        if st.button("Delete Column"):
            delete_column(column_to_delete)
            st.rerun()

    with st.expander("Plot Graph", expanded=False):
        # create an expander to plot the data
        df = fetch_data(f"SELECT * FROM {st.session_state.table_name}")
        column = st.selectbox("Select Column", df.columns)
        # get all columns of type date
        date_columns = [col for col in df.columns if "Datum" in col]
        date_column = st.selectbox("Select Date Column", date_columns)
        if st.button("Plot Graph"):
            fig = plt.figure()
            # sort values by date
            selected_df = df.sort_values(by=date_column)
            # drop rows with "" values
            selected_df = selected_df[selected_df[column] != ""]
            # drop all values that cant be converted to float
            selected_df = selected_df[selected_df[column].apply(lambda x: str(x).replace(".", "", 1).isdigit())]
            if selected_df.empty:
                st.write(f"No plottable data available for {column}")
                return
            y = selected_df[column]
            y = y.astype(float)
            X = selected_df[date_column]
            plt.plot(X, y, label=f"Werte {column}", marker='.')
            plt.title(f"Graph of {column} over time")
            plt.xlabel("Datum")
            plt.ylabel(column)
            # Rotate x-axis tick labels
            plt.xticks(rotation=45)
            plt.legend()
            fig_html = mpld3.fig_to_html(fig)
            components.html(fig_html, height=600)


# Run the Streamlit app
if __name__ == "__main__":
    main()
