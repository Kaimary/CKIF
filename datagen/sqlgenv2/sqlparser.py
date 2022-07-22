import random
import itertools
from weakref import ref

from .utils.helper import split_cls_into_chunks, split_into_clauses, \
    remove_duplicate_elms, replace_same_name_cols, add_join_conditions

AGG = ['COUNT', 'AVG', 'MAX', 'MIN', 'SUM']
MASKED_AGG = ['@COUNT', '@AVG', '@MAX', '@MIN', '@SUM']

class LowConfidenceSQLParser(object):
    def __init__(self, tables_file, trial):
        self._trial = trial
        self._tables_file = tables_file

    @property
    def where(self): 
        return self._where
    
    @property
    def order(self): 
        return self._order

    @property
    def is_valid_sql(self): 
        return self._is_valid_sql
    
    def load(self, sql, db_id, schema):
        self._sql = sql
        self._db_id = db_id
        self._schema = schema
        self._is_valid_sql = True

        self._select, self._from, self._where, self._group, self._order = \
            split_into_clauses(self._sql)

    def parse_all(self):
        """
        Globally check the sql

        1. One-table-sql: if the table is low-confidence and all referred columns have same-name columns in other table, replace the table;
        2. If low-confidence exists at `table`, and the group column is different with projection, forcely align group column with projection 
        """
        res = []

        if 'JOIN' not in self._from:
            toks = self._sql.split()
            columns = [tok.replace('@', '') for tok in toks if '.' in tok]
            if not columns: return res
            table = columns[0].split('.')[0]
            # Low confidence at table
            if f'@{table}' in self._from:
                other_tables = [t.lower() for t in self._schema.keys() if t.lower() != table]
                for ot in other_tables:
                    ot_columns = [c.lower()  for c in self._schema[ot]]
                    # If all the columns find same-name column in this table, replace the low-confidence table with this table
                    if all(c.split('.')[-1] in ot_columns for c in columns):
                        new_sql = self._sql.replace(table, ot).replace('@', '')
                        res.append(new_sql)
        if self._group and '@@' not in self._group and '@' in self._from:
            lc, hc, _ = split_cls_into_chunks(self._select, type='select')
            non_agg_column = [c.replace('@', '') for c in lc+hc if '(' not in c]
            # Assume only one non-aggregation-column exists
            if len(non_agg_column) == 1:
                gcol = self._group.split()[2].replace('@', '')
                if non_agg_column[0] != gcol:
                    self._group = self._group.replace(gcol, non_agg_column[0])

        return res

    def parse_from(self):
        """
        Parse the from clause of the sql. E.g. FROM A JOIN B ON A.a = B.b JOIN C ON B.b = C.c

        1. Remove relationship tables if not used in the clauses
        2. Remove low-confidence tables if reference columns have "same-name" columns in other tables.
        3. Add pk-fk-related table or remove replaceable table

        :return: list of referred table list, list of from clause string list, list of not used table list.

        #TODO support alias-included from sql
        """
        cls = self._from
        froms = []
        ref_tables_list = []
        not_used_tables_list = []    # All the relationship tables for linking purpose
        
        low_confidence_tables, high_confidence_tables, join_conditions = split_cls_into_chunks(cls, type="from")
        tables_in_from = low_confidence_tables + high_confidence_tables
        
        ref_tables = []   # All the tables used in the sql except those used in where subquery
        low_confidence_table_columns = [] 
        irreplaceable_tables = set()
        tokens =  ' '.join([self._select, self._where, self._group, self._order]).split()
        subquery = False
        for t in tokens:
            if t == '(SELECT': subquery = True
            if ')' in t and '(' not in t: subquery = False
            if '.' not in t or subquery: continue
            # Parse the columns into table name and column name respectively
            left = t.split('.')[0].replace('@', '')
            if '(' in left: left = left[left.index('(')+1: len(left)]
            right = t.split('.')[1].replace(')', '')
            
            if left not in ref_tables: ref_tables.append(left.lower())
            if left in low_confidence_tables and f"{left}.{right}" not in low_confidence_table_columns: 
                low_confidence_table_columns.append(f"{left}.{right}")
            # If no same-name column found in other tables, add the table of the current column
            if all(right != c for t in  self._schema.keys() for c in self._schema[t] if left !=t.lower()):
                irreplaceable_tables.add(left)
        if not ref_tables: ref_tables = tables_in_from

        # Revise incorrect predicted from-clause by adding missing tables if using in other clauses but not in from clause
        # TODO more than one missing table found
        # TODO skip to use the newly generated join conditions if the condition exists in the original clause 
        for t in ref_tables:
            if t not in cls:
                self._is_valid_sql = False
                s = f'FROM {tables_in_from[0] }'
                for tt in tables_in_from[1:]: s += f' JOIN {tt}'
                s += f' JOIN {t}'
                s, match = add_join_conditions(s, self._tables_file, self._db_id)
                if s:
                    if match: tables_in_from.append(t)
                    else: 
                        _, tables, _ = split_cls_into_chunks(s, type="from")
                        for tt in tables:
                            if tt not in tables_in_from: tables_in_from.append(tt)
                    tokens = cls.split()
                    tokens1 = s.split()
                    for i, t in enumerate(tokens1):
                        for tt in tokens:
                            if '@' in tt and tt[1:] == t:
                                tokens1[i] = f'@{t}'
                    cls = ' '.join(tokens1)
        tables_in_from.sort()

        not_used_tables = []
        if len(tables_in_from) > 1: not_used_tables = [t for t in tables_in_from if t not in ref_tables]

        # First add original from clause
        ref_tables_list.append(ref_tables)
        from_list = [cls.replace('@', '')]
        not_used_table_list = [not_used_tables]
        if self._is_valid_sql and '@' not in cls: return ref_tables_list, [from_list], [not_used_table_list]
        
        # Remove relationship tables if not used in the clauses
        if not_used_tables:
            for l in range(1, len(not_used_tables)+1):
                for ts in itertools.combinations(not_used_tables, l):
                    remove_tables = [ts[i] for i in range(len(ts))]
                    all_tables = tables_in_from.copy()
                    for t in remove_tables: all_tables.remove(t)
                    s = f"FROM {all_tables[0]}"
                    for t in all_tables[1:]: s += f" JOIN {t}"
                    s, match = add_join_conditions(s, self._tables_file, self._db_id)
                    if not match: continue
                    from_list.append(s)
                    not_used_table_list.append([t for t in not_used_tables if t not in remove_tables])
        #Add pk-fk-related table
        if '@FROM' in cls:
            other_tables = [t.lower() for t in self._schema.keys() if t.lower() not in tables_in_from]
            all_tables = tables_in_from.copy()
            s = f"FROM {all_tables[0]}"
            for t in all_tables[1:]: s += f" JOIN {t}"
            # Add pk-fk-related table 
            for t in other_tables:
                new_from = s + f" JOIN {t}"
                new_from, match = add_join_conditions(new_from, self._tables_file, self._db_id)
                if new_from and match:
                    from_list.append(new_from)
                    not_used_table_list.append([t])  
        # The above two cases share the same referred tables
        froms.append(from_list)
        not_used_tables_list.append(not_used_table_list)    

        # replaceable_tables = [t for t in tables_in_from if t not in irreplaceable_tables]
        # if '@from' in cls and replaceable_tables:
        #     # Remove replaceable table
        #     for rt in replaceable_tables:
        #             ref_tables_list.append([t for t in tables_in_from if t != rt])
        #             tables = [t for t in tables_in_from if t != rt]
        #             tables.sort()
        #             if tables:
        #                 from_ = f"from {tables[0]} "
        #                 for t in tables[1:]:
        #                     from_ += f"join {t} "
        #                 froms.append([from_])
        #                 not_used_tables_list.append([[]])
        
        # Remove low-confidence tables if reference columns have "same-name" columns in other tables.
        # if '@FROM' not in cls:
        for lconf_t in low_confidence_tables:
            replaceable = True
            all_high_confidence_table_columns = [f"{c.lower()}"  for t in high_confidence_tables for c in self._schema[t.lower()] if '(' not in c]
            for c in low_confidence_table_columns:
                if lconf_t in c and any(cc == c.split('.')[1] for cc in  all_high_confidence_table_columns): continue
                replaceable = False
            # Remove this low-confidence table
            if replaceable and low_confidence_table_columns:
                tables = tables_in_from.copy()
                tables.remove(lconf_t)
                ref_tables_list.append(tables)
                if tables:
                    s = f"FROM {tables[0]}"
                    for t in tables[1:]: s += f" JOIN {t}"
                    s, match = add_join_conditions(s, self._tables_file, self._db_id)
                    if match: 
                        froms.append([s])
                        not_used_tables_list.append([[]])
    
        return ref_tables_list, froms, not_used_tables_list

    # Parse SELECT with GROUP as they are semantically dependent
    def parse_select_group(self, tables, not_used_tables, schema):
        scls = self._select
        gcls = self._group
        scls = replace_same_name_cols(scls, tables, schema)
        gcls = replace_same_name_cols(gcls, tables, schema)
        # If no low-confidence exists, return
        if all('@' not in c for c in [scls, gcls]): return [scls +' ' + gcls]
        # If no grouping, only parse select
        if not gcls: return self.parse_select(tables, not_used_tables, schema)

        no_lconf_s = True if '@' not in scls else False
        no_lconf_g = True if '@' not in gcls else False

        # First check if exists the alignment between selection and grouping attribute 
        gcol = gcls.split()[2].replace('@', '') if '@@' not in gcls else ""
        low_conf_tokens, high_conf_tokens, _ = split_cls_into_chunks(scls, type='select')
        s_g_correlated = True \
            if (not no_lconf_s and gcol and any(gcol in t for t in low_conf_tokens + high_conf_tokens))\
                or (no_lconf_s and gcol and any(gcol in t for t in low_conf_tokens)) \
                    else False
            # TODO remove all other low-confidences if correlated column finds
        # may need to be improved
        res = []
        if s_g_correlated:
            # Get all possible replaceable columns 
            all_possible_columns = [f"{t.lower()}.{c.lower()}"  for t in tables for c in schema[t.lower()] if '(' not in c]
            all_possible_columns.extend([
                f"{t.lower()}.{c.lower()}" for t in not_used_tables for c in schema[t.lower()] \
                        if any(c.lower() in cc for cc in low_conf_tokens+[gcol])
            ])
            having = gcls[gcls.index('HAVING'): len(gcls)] if 'HAVING' in gcls else ""
            havings = [having.replace('@', '')]
            if '@' in having:
                tokens = having.split()
                having_col = tokens[1]
                having_sym = tokens[2]
                having_value = tokens[3]
                trial = 10
                while trial:
                    trial -= 1
                    if '@' in having_col:
                        t_col_r = random.choice(all_possible_columns)
                        while t_col_r == having_col.lower():
                            t_col_r = random.choice(all_possible_columns)
                        having_col = t_col_r
                    if '@' in having_sym:
                        all_symbols = ['>', '=', '<', '!=', '>=', '<=']
                        all_symbols.remove(having_sym.replace('@', ''))
                        having_sym = random.choice(all_symbols)
                    predicate = ' '.join(['HAVING', having_col, having_sym, having_value])
                    if predicate not in havings: havings.append(predicate)
                
            # First add orginal clause-string back
            # Removing duplicate selections before concatenating tokens
            original_cls = 'SELECT ' + \
                ' ,'.join(remove_duplicate_elms(high_conf_tokens + low_conf_tokens)) + \
                    ' ' + gcls.replace('@', '')
            res.append(' '.join(original_cls.split()))

            num_changable = True if '@SELECT' in scls else False
            num_sels = len(high_conf_tokens + low_conf_tokens)

            trial = self._trial
            low_conf_replacements = []
            low_conf_replacements_g = []
            low_conf_replacements_s_g = set()
            # Iterate over each low-confidence token and do the replacement
            while trial:
                trial -= 1
                replacement = []
                gcol_r = gcol.replace('@', '')
                for lc_t in low_conf_tokens:
                    align = True if gcol in lc_t else False
                    # Low confidence occurred at aggregation
                    if any(f'{w}(' in lc_t for w in MASKED_AGG):
                        agg = lc_t[lc_t.index('@')+1: lc_t.index('(')]
                        # all_aggs.remove(agg)
                        agg_r = random.choice(AGG + [''])
                        if agg_r: lc_t = lc_t.replace(f'@{agg}', agg_r)
                        else: lc_t = lc_t.replace(f'@{agg}(', '').replace(')', '')
                        if '@' in lc_t:
                            t_col = lc_t.split('@')[1].replace(')', '')
                            t_col_r = random.choice(all_possible_columns + ['*'])
                            while t_col_r == t_col.lower():
                                t_col_r = random.choice(all_possible_columns)
                            replacement.append(lc_t.replace(f'@{t_col}', t_col_r))
                            if align: gcol_r = t_col_r
                        else:
                            replacement.append(lc_t)
                    else:
                        t_col = lc_t.split('@')[1].replace(')', '')
                        if any(f'{agg}(' in lc_t for agg in AGG):
                            all_possible_columns += ['*']
                        t_col_r = random.choice(all_possible_columns)
                        while t_col_r == t_col.lower():
                            t_col_r = random.choice(all_possible_columns)
                        replacement.append(lc_t.replace(f'@{t_col}', t_col_r))
                        if align: gcol_r = t_col_r 
                # Random add/delete columns if low-confidence exists at sql keyword
                rdnum = random.choice(range(1, 4))
                if num_changable and rdnum != num_sels:
                    # Random add tokens
                    if rdnum > num_sels:
                        str = ""
                        if random.getrandbits(1):
                            str = f"{random.choice(AGG)}("
                        col = random.choice(all_possible_columns)
                        if str:  str += f'{col})'
                        else: str = f'{col}'
                        if str not in replacement: replacement.append(str)
                    # else TODO Random remove any of low-confidence tokens

                replacement.sort()
                gcol_r = f" GROUP BY {gcol_r}" if random.random() < 0.5 else ""
                if (' '.join(replacement) + gcol_r) not in low_conf_replacements_s_g:
                    low_conf_replacements.append(replacement)
                    low_conf_replacements_g.append(gcol_r)
                low_conf_replacements_s_g.add(' '.join(replacement) + gcol_r)
            # Construct the clause-string replacements
            for low_confs, gcol in zip(low_conf_replacements, low_conf_replacements_g):
                if gcol:
                    having = random.choice(havings)
                else:
                    having = ''
                str = 'SELECT ' + \
                    ' , '.join(remove_duplicate_elms(high_conf_tokens + low_confs)) + \
                        f'{gcol} {having}'
                if str not in res: res.append(' '.join(str.split()))
        else:
            # Low-confidence in GROUP but not in SELECT
            if no_lconf_s:    return self.parse_group(tables, not_used_tables, schema, prefix=scls)
            # Low-confidence in SELECT but not in GROUP
            elif no_lconf_g: return self.parse_select(tables, not_used_tables, schema, suffix=gcls)
            # Low-confidence both in SELECT and GROUP
            else:
            # Processing SELECT and GROUP separately
                selects = self.parse_select(tables, not_used_tables, schema)
                groups = self.parse_group(tables, not_used_tables, schema)
                for select in selects:
                    for group in groups:
                        str = ' '.join([select.strip(), group.strip()])
                        if str not in res: res.append(str)

        return res

    def parse_select(self, tables, not_used_tables, schema, suffix=""):
        cls = self._select
        if '@' not in cls: return [f'{cls} {suffix}']
    
        res = []
        high_conf_tokens = []
        low_conf_tokens = []
        # Split SELECT clause into each independent chunk
        low_conf_tokens, high_conf_tokens, _ = split_cls_into_chunks(cls, type='select')
        
        # First add orginal clause-string back
        # Removing duplicate selections before concatenating tokens
        original_cls = 'SELECT ' + \
            ' ,'.join(remove_duplicate_elms(high_conf_tokens + low_conf_tokens)) + \
                ' ' + suffix
        res.append(original_cls)

        # Get all possible replaceable columns 
        all_possible_columns = [f"{t.lower()}.{c.lower()}"  for t in tables for c in schema[t.lower()] if '(' not in c]
        low_conf_columns = [t.split('.')[1].replace(')', '') for t in low_conf_tokens if '.' in t]
        same_name_columns = [f"{t.lower()}.{c.lower()}" for t in not_used_tables for c in schema[t.lower()] if any(lc in c for lc in low_conf_columns)]
        all_possible_columns.extend(same_name_columns)
        # for t in high_conf_tokens:
        #     if '.' not in t: continue
        #     tab = t.split('.')[0].lower()
        #     # DISTINCT exists
        #     if ' ' in tab: tab = tab.split()[1]
        #     c  = t.split('.')[1].lower()
        #     if '(' in tab: tab = tab[tab.index('(')+1: len(tab)]
        #     if ' ' in tab: tab = tab[tab.index(' ')+1: len(tab)]
        #     if ')' in c: c = c.replace(')', '')
        #     if f"{tab}.{c}" in all_possible_columns: all_possible_columns.remove(f"{tab}.{c}")
        
        num_changable = True if '@SELECT' in cls else False
        num_sels = len(high_conf_tokens + low_conf_tokens)

        trial = self._trial
        low_conf_replacements = []
        # Iterate over each low-confidence token and do the replacement
        while trial:
            trial -= 1
            replacement = []
            for lc_t in low_conf_tokens:
                # Low confidence occurred at aggregation
                if any(f'{w}(' in lc_t for w in MASKED_AGG):
                    agg = lc_t[lc_t.index('@')+1: lc_t.index('(')]
                    # all_aggs.remove(agg)
                    agg_r = random.choice(AGG + [''])
                    if agg_r: lc_t = lc_t.replace(f'@{agg}', agg_r)
                    else: lc_t = lc_t.replace(f'@{agg}(', '').replace(')', '')
                    if '@' in lc_t:
                        t_col = lc_t.split('@')[1].replace(')', '')
                        t_col_r = random.choice(all_possible_columns + ['*'])
                        # TODO: ---------------------------------- FOR DEBUG ----------------------------------
                        while t_col_r == lc_t.lower():
                        # TODO: ********************************** END DEBUG **********************************
                            t_col_r = random.choice(all_possible_columns + ['*'])
                        replacement.append(lc_t.replace(f'@{t_col}', t_col_r))
                    else:
                        replacement.append(lc_t)
                else:
                    t_col = lc_t.split('@')[1].replace(')', '')
                    # @table.column => 1. agg(table.column) 2. table.column1
                    if all(f'{agg}(' not in lc_t for agg in AGG) and random.getrandbits(1):
                        agg = random.choice(AGG)
                        replacement.append(lc_t.replace(f'@{t_col}', f'{agg}({t_col})'))
                    else:
                        t_col_r = random.choice(all_possible_columns + ['*']) \
                            if any(f'{agg}(' in lc_t for agg in AGG) \
                                else random.choice(all_possible_columns)
                        while t_col_r == t_col.lower():
                            t_col_r = random.choice(all_possible_columns)
                        replacement.append(lc_t.replace(f'@{t_col}', t_col_r))
            # Random add/delete columns if low-confidence exists at sql keyword
            rdnum = random.choice(range(1, 4))
            if num_changable and rdnum != num_sels:
                # Random add tokens
                if rdnum > num_sels:
                    str = ""
                    if random.getrandbits(1):
                        str = f"{random.choice(AGG)}("
                    col = random.choice(all_possible_columns)
                    if str:  str += f'{col})'
                    else: str = f'{col}'
                    if str not in replacement: replacement.append(str)
                # TODO 
                # Random remove any of low-confidence tokens

            replacement.sort()
            if replacement not in low_conf_replacements: 
                low_conf_replacements.append(replacement)

        # Construct the clause-string replacements
        res.extend(['SELECT ' + \
            ' ,'.join(remove_duplicate_elms(high_conf_tokens + low_confs)) + ' ' + suffix
                for low_confs in low_conf_replacements])

        return res
    
    def parse_group(self, tables, not_used_tables, schema, prefix = ""):
        cls = self._group
        if '@' not in cls: return [cls]

        res = []
        res.append(f'{prefix}')

        # Generate grouping
        # TODO only support generate non-having grouping
        if '@@GROUP' in cls:
            # Get all possible replaceable columns 
            all_possible_columns = [f"{t.lower()}.{c.lower()}"  for t in tables for c in schema[t.lower()] if '(' not in c]
            trial = 100
            while trial:
                trial -= 1
                t_col_r = random.choice(all_possible_columns)
                replacement = f'{prefix} GROUP BY {t_col_r}'
                if replacement not in res: res.append(replacement)
            return res
        
        res.append(f"{prefix} {cls.replace('@', '')}")
        # Get all possible replaceable columns 
        all_possible_columns = [f"{t.lower()}.{c.lower()}"  for t in tables for c in schema[t.lower()] if '(' not in c]
        tokens = cls.split()
        gcols = [tokens[2].replace('@', '')]
        havings = [' '.join(tokens[3:]).replace('@', '')] if len(tokens) > 3 else [""]

        # If low-confidence at grouping column
        if '@' in tokens[2]:
            trial = 100
            # Iterate over each low-confidence token and do the replacement
            while trial:
                trial -= 1
                # Low confidence occurred at aggregation
                t_col = tokens[2].replace('@', '')
                t_col_r = random.choice(all_possible_columns)
                while t_col_r == t_col.lower():
                    t_col_r = random.choice(all_possible_columns)
                if t_col_r not in gcols: 
                    gcols.append(t_col_r)
        # If low-confidence at group keyword, having may need to be considered.
        if '@GROUP' in cls: 
            havings.append("")
            # If not exists having clause, random generate
            if 'HAVING' not in cls:
                trial = 10
                while trial:
                    trial -= 1
                    agg = 'COUNT'
                    symbol = random.choice(['>', '<', '>=', '<='])
                    str = f"HAVING {agg}(*) {symbol} 'terminal'"
                    if str not in havings: havings.append(str)
            # If exists having clause and has low-confidence
            elif '@' in cls[cls.index('HAVING'):]:
                assert len(tokens) == 7
                having_col = tokens[4]
                having_sym = tokens[5]
                having_value = tokens[6]
                trial = 10
                while trial:
                    trial -= 1
                    if '@' in having_col:
                        t_col_r = random.choice(all_possible_columns)
                        while t_col_r == having_col.lower():
                            t_col_r = random.choice(all_possible_columns)
                        having_col = t_col_r
                    if '@' in having_sym:
                        all_symbols = ['>', '=', '<', '!=', '>=', '<=']
                        all_symbols.remove(having_sym.replace('@', ''))
                        having_sym = random.choice(all_symbols)
                    predicate = ' '.join([having_col, having_sym, having_value])
                    if predicate not in havings: havings.append(predicate)
        # composing
        for gcol in gcols:
            for having in havings:
                res.append(f'{prefix} GROUP BY {gcol} {having}')

        return res

    def parse_order(self, tables, not_used_tables, schema):
        cls = self._order
        cls = replace_same_name_cols(cls, tables, schema)
        if '@' not in cls: return [cls]

        res = []
        res.append("")
        # Add non-order first
        if '@ORDER' in cls: 
            # Generate odering
            if '@@ORDER' in cls:
                # Get all possible replaceable columns 
                all_possible_columns = [f"{t.lower()}.{c.lower()}"  for t in tables for c in schema[t.lower()] if '(' not in c]
                # TODO: ---------------------------------- FOR DEBUG ----------------------------------
                trial = 100
                # TODO: ********************************** END DEBUG **********************************
                while trial:
                    trial -= 1
                    toks = ["ORDER", "BY"]
                    col = random.choice(all_possible_columns)
                        # TODO: ---------------------------------- FOR DEBUG ----------------------------------
                    if random.random() < 0.4:
                        # No Order by AVG\MAX\MIN
                        # all_aggs = ['COUNT', 'AVG', 'MAX', 'MIN', 'SUM']
                        all_aggs = ['COUNT', 'SUM']
                        # TODO: ********************************** END DEBUG **********************************
                        agg = random.choice(all_aggs)
                        toks.append(f'{agg}({col})')
                    else: toks.append(col)
                    # TODO: ---------------------------------- FOR DEBUG ----------------------------------
                    toks.append(random.choice(['DESC', 'ASC']))
                    # if random.getrandbits(1): toks.append(random.choice(['DESC', 'ASC']))
                    # TODO: ********************************** END DEBUG **********************************
                    if random.getrandbits(1): toks.append('LIMIT 1')
                    o = ' '.join(toks)
                    if o not in res: res.append(o)
            else:
                res.append(cls.replace('@', ''))
        else:
            res.append(cls.replace('@', ''))
            tokens = cls.split()
            ocol = cls.split()[2]
            limit = ' '.join(tokens[3:]) if len(tokens) > 3 else ""
            asc = ""
            # ASC LIMIT 1
            if len(limit.split()) == 3:
                asc = limit.split()[0]
                limit = limit[limit.index(' ')+1:]
            # ASC
            elif len(limit.split()) == 1:
                asc = limit
                limit = ""
            # Get all possible replaceable columns 
            all_possible_columns = [
                f"{t.lower()}.{c.lower()}" for t in tables for c in schema[t.lower()] if '(' not in c
            ] + ['*']
            trial = 100
            ocol_replacements = [ocol.replace('@', '')]
            # Iterate over each low-confidence token and do the replacement
            if '@' in ocol:
                while trial:
                    trial -= 1
                    tok = ""
                    # Low confidence occurred at aggregation
                    t_col = ocol.split('@')[1]
                    t_col_r = random.choice(all_possible_columns)
                    while t_col_r == t_col.lower():
                        t_col_r = random.choice(all_possible_columns)
                    bagg = False
                    if random.getrandbits(1):
                        agg = random.choice(['COUNT', 'SUM'])
                        bagg = True
                        tok = f'{agg}('
                    if bagg: 
                        tok += f'{t_col_r})'
                    else:
                        tok = f'{t_col_r}'

                    if t_col_r not in ocol_replacements: 
                        ocol_replacements.append(tok)
            asc_replacements = ['DESC', 'ASC'] if '@' in asc else [asc]
            limit_replacements = [limit.replace('@', ''), ''] if '@' in limit else [limit]
            # Construct the clause-string replacements
            for col in ocol_replacements:
                for a in asc_replacements:
                    for l in limit_replacements:
                        if f'ORDER BY {col} {a} {l}' not in res and col != '*':
                            res.append(f'ORDER BY {col} {a} {l}')

        return res

    def parse_where(self, tables, not_used_tables, schema):
        cls = self._where
        cls = replace_same_name_cols(cls, tables, schema)
        # Extra step for GAP
        # add join condition if where contains subquery
        if ' JOIN ' in cls:
            f_sp = cls.index('FROM')
            wcls_1 = cls[:f_sp]
            f_ep = len(cls) - 1
            if 'WHERE' in cls[f_sp:]:
                f_ep = cls.rindex('WHERE')
            elif 'GROUP' in cls[f_sp:]:
                f_ep = cls.rindex('GROUP')
            elif 'ORDER' in cls[f_sp:]:
                f_ep = cls.rindex('ORDER')
            wcls_2, _ = add_join_conditions(cls[cls.index('FROM'):f_ep].replace('@', ''), self._tables_file, self._db_id)
            wcls_3 = cls[f_ep:]
            cls = ' '.join([wcls_1, wcls_2, wcls_3])
        # cls = add_join_conditions(cls)
        if '@' not in cls: return [cls]

        res = []
        res.append("")
        # Get all possible replaceable columns 
        all_possible_columns = [f"{t.lower()}.{c.lower()}"  for t in tables for c in schema[t.lower()] if '(' not in c]
        # Add non-where first
        if '@WHERE' in cls: 
            # Generate where
            if '@@WHERE' in cls:
                trial = 100
                while trial:
                    trial -= 1
                    t_col_r = random.choice(all_possible_columns)

                    all_symbols = ['>', '=', '<', '!=', '>=', '<=']
                    symbol = random.choice(all_symbols)

                    if f"WHERE {t_col_r} {symbol} 'terminal'" not in res: res.append(f"WHERE {t_col_r} {symbol} 'terminal'")
            else:
                res.append(cls.replace('@', ''))
        # 1. low-confidence columns
        # 2. low-confidence assignments
        # 3. low-confidence conjunctions
        ##TODO 4. low-confidence values
        # TODO: ---------------------------------- FOR DEBUG ----------------------------------
        if ' @' in cls.strip():
        # TODO: ********************************** END DEBUG **********************************
            # res.append(cls.replace('@', ''))
            high_conf_tokens = []
            low_conf_tokens = []
            conj_tokens = []
            # Split clause into each independent token
            low_conf_tokens, high_conf_tokens, conj_tokens = split_cls_into_chunks(cls, type='where')
            low_conf_tokens.sort()
            # Construct the clause-string replacements
            str = 'where '
            conj_idx = 0
            idx = 0
            for t in (high_conf_tokens+low_conf_tokens):
                idx += 1
                if idx > 1:  
                    str += ' ' + conj_tokens[conj_idx].replace('@', '')
                    conj_idx += 1
                str += ' ' + t.replace('@', '')
            res.append(str)
            # First check low-confidence conjunction 
            # assume that there is at most one this conjunction
            if any('@' in c for c in conj_tokens):
                assert len(high_conf_tokens+low_conf_tokens) == 2

                predicates = [p.replace('@', '') for p in low_conf_tokens + high_conf_tokens]
                res.append(f'WHERE {predicates[0]}')
                res.append(f'WHERE {predicates[1]}')
            # else:
            low_conf_replacements = []
            trial = self._trial
            while trial:
                trial -= 1
                replacement = []
                for lc_t in low_conf_tokens:
                    toks = lc_t.split()
                    t_col = toks[0]
                    symbol = toks[1]
                    # not in
                    if toks[1] == 'NOT': symbol = ' '.join(toks[1:3])
                    value = toks[len(toks) - 1]

                    predicate_add = ""
                    if '@' in t_col:
                        t_col_r = random.choice(all_possible_columns)
                        # TODO: ---------------------------------- FOR DEBUG ----------------------------------
                        # while t_col_r == t_col.lower():
                        #     t_col_r = random.choice(all_possible_columns)
                        # TODO: ********************************** END DEBUG **********************************
                        t_col = t_col_r
                    if '@' in symbol:
                        # TODO: ---------------------------------- FOR DEBUG ----------------------------------
                        all_symbols = ['>', '=', '<', '!=', '>=', '<=', "BETWEEN 'terminal' AND"]
                        # TODO: ********************************** END DEBUG **********************************
                        # Generate one more predicate if symbol is low-confidence
                        if random.getrandbits(1) and random.getrandbits(1):
                            symbol = symbol.replace('@', '')
                            t_col_a = random.choice(all_possible_columns)
                            symbol_a = random.choice(all_symbols)
                            predicate_add = f'AND {t_col_a} {symbol_a} {value}'
                        else:
                            # TODO: ---------------------------------- FOR DEBUG ----------------------------------
                            # if 'LIKE' not in symbol: all_symbols.remove(symbol.replace('@', ''))
                            # TODO: ********************************** END DEBUG **********************************
                            symbol = random.choice(all_symbols)
                    elif any('@' in c for c in conj_tokens):
                        all_symbols = ['>', '=', '<', '!=', '>=', '<=', "BETWEEN 'terminal' AND"]
                        if random.getrandbits(1) and random.getrandbits(1):
                            symbol = symbol.replace('@', '')
                            t_col_a = random.choice(all_possible_columns)
                            symbol_a = random.choice(all_symbols)
                            if any(f"{t_col_a} {symbol_a} {value}" in h for h in high_conf_tokens):
                                predicate_add = ''
                            else:
                                predicate_add = f'AND {t_col_a} {symbol_a} {value}'

                    if predicate_add: predicate = ' '.join([t_col, symbol, value, predicate_add])
                    elif symbol == 'BETWEEN': predicate = ' '.join([t_col, symbol, value, 'AND', value])
                    else: predicate = ' '.join([t_col, symbol, value])
                    if predicate not in (replacement + high_conf_tokens): replacement.append(predicate)
                    replacement.sort()
                    if replacement not in low_conf_replacements:
                        low_conf_replacements.append(replacement)
            # Construct the clause-string replacements
            template = 'WHERE '
            conj_idx = 0
            idx = 0
            for t in high_conf_tokens:
                idx += 1
                if idx > 1:
                    template += ' ' + conj_tokens[conj_idx].replace('@', '')
                    conj_idx += 1
                template += ' ' + t

            for rep in low_conf_replacements:
                template1 = template
                idx1 = idx
                conj_idx1 = conj_idx
                for tt in rep:
                    idx1 += 1
                    if idx1 > 1:
                        template1 += ' ' + conj_tokens[conj_idx1].replace('@', '')
                        conj_idx1 += 1
                    template1 += ' ' + tt

                res.append(template1)

        return res