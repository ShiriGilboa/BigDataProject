import sqlite3


def get_columns_from_prop(element):
    # Get the properties of the class
    properties = vars(element)

    # Filter out the built-in properties and methods
    properties = [f"'{prop}'" for prop in properties.keys() if not prop.startswith("__")]

    # Concatenate the properties into a single string, separated by a comma
    property_string = ",".join(properties)

    return property_string


def get_values_from_element(element):
    slash = '\''

    # Get the values of the properties of the object
    values = [str(value) if not isinstance(value, str) else f"'{value.replace(slash, '')}'" for value in
              vars(element).values()]

    # Concatenate the values into a single string, separated by a comma, replace None with empty str
    value_string = (",".join(values)).replace('None', '\'\'')

    return value_string


class BaseHandler:
    def __init__(self, name, db, data):
        self.name = name
        self.db = db
        self.data = data
        self.conflicts = ""

    def create_table(self, table_spec):
        try:
            self.db.execute("{}".format(table_spec))
            print("Table created successfully or already exists")
        except sqlite3.OperationalError as ex:
            print("Probably Table with name {} already exists {}".format(self.name, ex))
        except Exception as ex:
            print("Got Exception when trying to create Table {}".format(ex))
        finally:
            self.db.commit()

    def handle_updated(self, key, values):
        """
        This method will create the qeury of relevant features to be updated if needed according to KEY
        :param key: The key we want to look for a conflict with
        :param values: list of params we want to update if there is a conflicts
        :return: an "On conflict Do update" string for the query
        """
        '''ON CONFLICT({}) DO UPDATE'''.format(key)
        set_vals = '''ON CONFLICT({}) DO UPDATE\n'''.format(key)
        for val in values:
            set_vals += "SET {val} = excluded.{val}\n".format(val=val)
        print("Created Conflicts handler {}".format(self.conflicts))
        self.conflicts = set_vals

    def insert_elements(self, elements):
        '''
        This Method will insert all data rows one by one, and update relevant features if needed
        :param elements: Elements object implemented as list of objects with desire data type
        :return: None
        '''
        if len(elements) > 0:
            # Extract the name of the properties for the column name.
            columns = get_columns_from_prop(elements[0])
            for element in elements:
                try:
                    # Execute and commit the command
                    insert_line = f'''
                                INSERT INTO {self.name} ({columns})
                                VALUES ({get_values_from_element(element)})
                                {self.conflicts}'''
                    print(insert_line)
                    self.db.execute(insert_line)
                except Exception as e:
                    print(f'Error: {e}')
                finally:
                    self.db.commit()
