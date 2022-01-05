LEVEL = "Level "
ID = " - ID"

class NodeOperator:
    """ Responsible for modifying the node such as adding
        additional columns or combining rows together """
    def calc_max_depth(self, headers):
        depth = 0
        for i in headers:
            if "id" in i.lower():
                depth += 1
        return depth

    def create_depth_node(self, max_depth, rows, columns):
        dct = {}
        for i in range(max_depth, 0, -1):
            cols = self.get_column_names(columns, i)
            if i == 1:
                node = self.get_first_node(rows, cols[0],
                                           cols[1], cols[2])
                dct[i] = node
            else:
                pid = self.get_parent_column(i)
                node = self.get_node(rows, pid, cols[0],
                                     cols[1], cols[2])
                dct[i] = node
        return dct

    def get_column_names(self, headers, depth):
        pattern = str(depth) + str(" ")
        cols = [col for col in headers if pattern in col]
        for col in cols:
            if "name" in col.lower():
                name = col
            if "url" in col.lower():
                url = col
            if "id" in col.lower():
                id = col
        return name, id, url

    def aggregate_columns(self, rows):
        """ Combines the column elements into a single dictionary.
            Then creates a dictionary of list of these values sharing
            the same parent 
        eg. dct[pid] = [{"lbl":"a", "ID":"1", "link":"link_1", "children":[]},
                        {"lbl":"b", "ID":"2", "link":"link_2", "children":[]}] """
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
        return LEVEL + str(depth - 1) + str(ID)

    def get_node(self, rows, parent, name, id, url):
        """ Creates a dict using tuple of pid and id as key to account 
            for node having mulitple parents. The dict value is of format 
            {"pid", "label", "ID" , "link", "children"} """
        node = {}
        for row in rows:
            row_id = row[id]
            if row_id:
                node[(row_id, row[parent])] = {"pid":row[parent],
                                               "label":row[name],
                                               "ID":row[id],
                                               "link":row[url],
                                               "children":[]}
        return node

    def combine_nodes(self, node, max_depth):
        """ Combine the nodes of different levels having a parent to 
            child relation. The child rows having the same parent will be 
            combined into a list of rows. """
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