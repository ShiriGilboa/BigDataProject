import sqlite3


# Extract the sets as {key} = {value}
def get_sets(element):
    slash = '\''
    updates = []

    keys = [f"{prop}" for prop in vars(element).keys() if not prop.startswith("__")]

    values = [str(value) if not isinstance(value, str) else f"'{value.replace(slash, '')}'" for value in
              vars(element).values()]

    for k, v in zip(keys, values):
        updates.append(f" {k} = {v} ")

    update_string = (",".join(updates)).replace('None', '\'\'')
    return update_string


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
    def __init__(self, name, db, data, cache=None):
        self.name = name
        self.db = db
        self.data = data
        self.conflicts = ""
        self.new_data = []
        self.need_update = []
        self.split_data(cache)

    def split_data(self, cache):
        '''
        Here, using our cache (if exists) we are going to split the data into 2 groups. the need to be updated data and
        new data.
        This method going to set 2 members : new_data, need_update_data
        '''
        #Cheching that cache exists, and that desired data and we relevant cache there.
        if cache and hasattr(cache, self.name.lower()) and cache.__getattribute__(self.name.lower()) is not None:
            for element in self.data:
                #exists completely no need to update it
                if element in cache.__getattribute__(self.name.lower()):
                    continue
                # this data is not exists / changed
                elif hasattr(self, "key_attr") and self.key_attr:
                    checked_keys = False
                    for cache_elem in cache.__getattribute__(self.name.lower()):
                        # checking if this item is exists in cache according to keys(assuming keys wont change)
                        checked_keys = all([element.__getattribute__(key)==cache_elem.__getattribute__(key) for key in self.key_attr])
                        if checked_keys:
                            self.need_update.append(element)
                else:
                    # need to insert this column
                    self.new_data.append(element)
        else:
            self.new_data = self.data


    def create_table(self, table_spec):
        print("Trying to create table with name {}".format(self.name))
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

    def insert_elements(self):
        '''
        This Method will insert all data rows one by one, and update relevant features if needed
        :param elements: Elements object implemented as list of objects with desire data type
        :return: None
        '''
        if len(self.new_data) > 0:
            # Extract the name of the properties for the column name.
            columns = get_columns_from_prop(self.new_data[0])
            for element in self.new_data:
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

    def where_condition(self):
        raise NotImplementedError
    def update_data(self):
        if len(self.need_update) > 0:
            total_num = len(self.need_update)
            # Connect to the db.
            with sqlite3.connect(self.db) as conn:
                queries = ''
                for element in self.need_update:
                    try:
                        # Execute and commit the command
                        conn.execute(f'''UPDATE {self.name} SET {get_sets(element)}
                                        WHERE {self.where_condition(element)}''')

                        conn.commit()
                    except Exception as e:
                        print(f'Error: {e}')
