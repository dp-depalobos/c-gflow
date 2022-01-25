LEVEL = "Level "
ID = " - ID"

class NodeOperator:
    """ Responsible for modifying the node such as adding
        additional columns or combining rows together """

    def create_depth_node(self, max_depth, rows, columns):
        """ Creates collection of level to Node. 
            A node consists of Name, ID and URL for the first node.
            And Parent ID, Name, ID, and URL for subsequent nodes."""
        depth_node_dct = {}
        for i in range(max_depth, 0, -1):
            column_names = self.get_column_names(columns, i)
            if i == 1:
                node = self.get_first_node(rows, *column_names)
                depth_node_dct[i] = node
            else:
                pid = self.get_parent_column(i)
                node = self.get_node(rows, pid, *column_names)
                depth_node_dct[i] = node
        return depth_node_dct

    def get_column_names(self, headers, depth):
        pattern = str(depth) + str(" ")
        columns = [column for column in headers if pattern in column]
        for column in columns:
            if "name" in column.lower():
                name = column
            if "url" in column.lower():
                url = column
            if "id" in column.lower():
                id = column
        return name, id, url

    def aggregate_columns(self, rows):
        """ Combines the column elements into a single dictionary,
            then creates a dictionary of list of these values sharing
            the same parent """
        dct = {}
        for row in rows:
            pid = row["pid"]
            if pid and pid in dct:
                dct[pid].append({"label":row["label"],
                                 "ID":row["ID"],
                                 "link":row["link"],
                                 "children":row["children"]})
            else:
                dct[pid] = [{"label":row["label"],
                             "ID":row["ID"],
                             "link":row["link"],
                             "children":row["children"]}]
        return dct

    def get_first_node(self, rows, name, id, url):
        """ Creates a dict using ID as key. The value is of format 
            {"label", "ID" , "link", "children"}. Since it is the 
            highest node, there is no pid """
        node = {}
        for row in rows:
            row_id = row[id]
            if row_id:
                node[row_id] = {"label":row[name],
                                "ID":row[id],
                                "link":row[url],
                                "children":[]}
        return node

    def get_parent_column(self, depth):
        return LEVEL + str(depth - 1) + ID

    def get_node(self, rows, parent, name, id, url):
        """ Creates a dict using tuple of pid and id as key to account 
            for node having mulitple parents. The dict value is of format 
            {"pid", "label", "ID" , "link", "children"} """
        node = {}
        for row in rows:
            row_id = row[id]
            pid = row[parent]
            if row_id and (row_id, pid) not in node:
                node[(row_id, pid)] = {"pid":row[parent],
                                       "label":row[name],
                                       "ID":row[id],
                                       "link":row[url],
                                       "children":[]}
        return node

    def combine_nodes(self, node, max_depth):
        """ Combine the nodes of different levels having a parent to 
            child relation. The child rows having the same parent will be 
            aggregated into a list of rows. """
        end_node = []
        for i in range(max_depth, 0, -1):
            if max_depth == i:
                parent = list(node[i].values())
                end_node = self.aggregate_columns(parent)            
            elif i > 1:
                parent = list(node[i].values())
                child = end_node
                joined = self.join(parent, child)
                end_node = self.aggregate_columns(joined)
            else:
                parent = list(node[i].values())
                child = end_node
                end_node = self.join(parent, child)
        return end_node

    def join(self, parent, node):
        """ Matches parent node to the child aggregate having the same pid """
        for row in parent:
            pid = row["ID"]
            if pid in node:
                children = node[pid]
                row["children"] = children
        return parent