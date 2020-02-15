
from enum import Enum
# from sdl.rdfmt_extractor import RDFMTExtractor

from awudima.sdesc.utils import contact_sparql_endpoint


class Federation:
    """Description of data source federation

    It represents a virtual view over group of data sources in a semantic data lake.
    This class represents a set of data sources in a semantic data lake and provides a virtual view over them.
    """

    def __init__(self, fedId, name, desc):
        """

        :param fedId: str ID of the federation
        :param name: short name of the federation
        :param desc: short description of the intended domain or project the federation will serve
        """

        self.fedId = fedId
        self.name = name
        self.desc = desc
        self.datasources = set()
        self.rdfmts = set()

    def extract_molecules(self, merge=True):
        """extract RDFMT for this federation

        :param merge: whether to merge or not - replace. default True
        :return:
        """
        extractor = RDFMTExtractor()
        if merge:
            self.rdfmts = set()
        rdfmts_dict = self.rdfmts_as_dict_obj()

        for ds in self.datasources:
            mts = extractor.get_molecules(ds, collect_labels=True, collect_stats=True)
            for m in mts:
                if m.mtId in rdfmts_dict:
                    rdfmts_dict[m.mtId].merge_with(m)
                else:
                    self.rdfmts.add(m)

        return self.rdfmts

    def extract_source_molecules(self, datasource, merge=True):
        """extract RDFMT for this federation

        :param merge: whether to merge or not - replace. default True
        :return:
        """
        extractor = RDFMTExtractor()
        if merge:
            toremove = []
            for m in self.rdfmts:
                if datasource in m.datasources:
                    # if this rdfmt is not available in any other data sources, then remove it completely,
                    # else just remove the datasouce from sources list
                    if len(m.datasources) == 1:
                        toremove.append(m)
                    else:
                        m.datasources.remove(datasource)

            for m in toremove:
                self.rdfmts.remove(m)

        # self.rdfmts.update(extractor.get_molecules(datasource, collect_labels=True, collect_stats=True))
        mts = extractor.get_molecules(datasource, collect_labels=True, collect_stats=True)
        rdfmts_dict = self.rdfmts_as_dict_obj()
        for m in mts:
            if m.mtId in rdfmts_dict:
                rdfmts_dict[m.mtId].merge_with(m)
            else:
                self.rdfmts.add(m)

        return self.rdfmts

    def to_str(self):
        """Produces a text representation of the federation

        :return: text representation as fedId(name)
        """

        return self.fedId

    def addSource(self, source):
        self.datasources.add(source)

    def addRDFMT(self, rdfmt):
        rdfmts_dict = self.rdfmts_as_dict_obj()
        if rdfmt.mtId in rdfmts_dict:
            rdfmts_dict[rdfmt.mtId].merge_with(rdfmt)
        else:
            self.rdfmts.add(rdfmt)

    def addRDFMTs(self, rdfmts):
        # self.rdfmts.update(rdfmts)
        rdfmts_dict = self.rdfmts_as_dict_obj()
        for rdfmt in rdfmts:
            if rdfmt.mtId in rdfmts_dict:
                rdfmts_dict[rdfmt.mtId].merge_with(rdfmt)
            else:
                self.rdfmts.add(rdfmt)

    def rdfmts_as_dict(self):
        return {r.mtId: r.to_json() for r in self.rdfmts}

    def rdfmts_as_dict_obj(self):
        return {r.mtId: r for r in self.rdfmts}

    def to_json(self):
        """Produces a JSON representation of the federation

        :return: json representation of the Federation
        """

        return {
            "fedId": self.fedId,
            "name": self.name,
            "desc": self.desc,
            'rdfmts': [r.to_json() for r in self.rdfmts],
            "sources": [s.to_json() for s in self.datasources]
        }

    def __str__(self):
        return self.to_str()

    def __repr__(self):
        return self.to_str()

    def __hash__(self):
        return hash(self.fedId)


class DataSource:
    """ Data source descriptions

    Represents a data source in a semantic data lake. A data source is identified by its id and url.
    """

    def __init__(self, dsId, dstype, url, name, desc='', acronym='', params=None):
        """

        :param dsId:
        :param dstype: type of the source system: one of
                        [SPARQL_Endpoint, MySQL, Postgres, MongoDB, Neo4j, HADOOP_CSV, HADOOP_XML,
                         HADOOP_JSON, HADOOP_TSV, SPARK_CSV, SPARK_TSV, SPARK_XML, SPARK_JSON,
                         REST, CSV, TSV, JSON, XML, TXT]
        :param url: path to the dataset/file/api. Could be file://.., hdfs://.., http://.., ftp://..
        :param name: name of the datasource
        :param desc: short description of the data stored in this data source
        :param acronym: short acronym, if available
        :param params: a key-value pair of other configuration parameters
        """

        self.dsId = dsId
        self.name = name
        self.desc = desc
        self.dstype = dstype
        self.url = url
        self.params = params
        self.policy = None

    def to_str(self):
        """Produces a text representation of this data source

        :return: text representation as dsId(name)
        """

        return self.dsId

    def to_json(self):
        """Produces a JSON representation of this data source

        :return: json representation of this data source
        """

        return {
            "name": self.name,
            "dsId": self.dsId,
            "url": self.url,
            "dstype": self.dstype.value,
            "params": self.params,
            "desc": self.desc
        }

    def __str__(self):
        return self.to_str()

    def __repr__(self):
        return self.to_str()

    def __eq__(self, other):
        return self.dsId == other.dsId and self.url == other.url

    def __hash__(self):
        return hash(self.dsId + '-' + self.url)


class RDFMT:
    """Represents an RDF molecule template (RDF-MT)

    An RDF-MT is an abstract description of semantic concepts represented in a dataset.
    It is identified by its unique mtID (IRI), representing a class/concept in an ontology and it comprises a set of
    possible properties/predicates an instance of this class can have. Instances of an RDF-MT can be available in
    one or more data sources in a federation.
    """

    def __init__(self, mtId, label, mttype, desc='', cardinality=-1):
        """

        :param mtId: unique id of the molecule template (class/concept)
        :param label: name/label of the molecule template
        :param mttype: whether it is a typed RDF-MT or not. Typed RDF-MTs are RDF-MTs that are defined by the ontology
                    and can be extracted using via 'instance of' property (rdf:type/P31) of instances.
        """

        self.mtId = mtId
        self.label = label
        self.mttype = mttype
        self.desc = desc
        self.predicates = set()
        self.datasources = set()
        self.cardinality = cardinality
        self.subClassOf = []
        self.constraints = []
        self.policy = None

    def addPredicate(self, pred):
        self.predicates.add(pred)

    def preds_as_dict(self):
        return {p.predId: p.to_json() for p in self.predicates}

    def preds_as_dict_obj(self):
        return {p.predId: p for p in self.predicates}

    def addDataSource(self, ds):
        self.datasources.add(ds)

    def to_str(self):
        """Produces a textual representation of the molecule template

        :return: text representation as mtId(label)
        """

        return self.mtId

    def to_json(self):
        """Produces a JSON representation of the molecule template

        :return: json representation of the molecule template
        """

        return {
            "mtId": self.mtId,
            "mttype": self.mttype,
            'label': self.label,
            'desc': self.desc,
            'cardinality': self.cardinality,
            "subClassOf": self.subClassOf,
            "predicates": [p.to_json() for p in self.predicates],
            "datasources": [d.to_json() for d in self.datasources],
            "constraints": [c for c in self.constraints]
        }

    def merge_with(self, other):
        if self.mtId != other.mtId:
            raise Exception("Cannot merge two different RDFMTs " + self.mtId + ' and ' + other.mtId)
        merged = RDFMT(self.mtId, self.label, self.mttype, self.desc, self.cardinality)
        if self.label is None or len(self.label) == 0:
            merged.label = other.label
        if self.desc is None or len(self.desc) == 0:
            merged.desc = other.desc
        if self.cardinality == -1:
            merged.cardinality = other.cardinality

        merged.subClassOf.extend(other.subClassOf)
        merged.subClassOf = list(set(merged.subClassOf))

        otherpreds = other.preds_as_dict_obj()
        mergedpreds = []
        difs = self.predicates.difference(other.predicates)
        for p in self.predicates:
            if p.predId in otherpreds:
                mergedpreds.append(p.merge_with(otherpreds[p.predId]))
            else:
                mergedpreds.append(p)

        for p in difs:
            mergedpreds.append(p)

        merged.predicates = list(set(mergedpreds))
        merged.datasources = self.datasources | other.datasources
        # TODO: merge constaints and access policies (restriced first approach)

        return merged

    def __str__(self):
        return self.to_str()

    def __repr__(self):
        return self.to_str()

    def __eq__(self, other):
        return self.mtId == other.mtId

    def __hash__(self):
        return hash(self.mtId)


class Predicate:
    """Represents predicates of an RDF molecule template

    A predicate/property of a molecule template represents a single data point associated to molecule (an instance of a
    molecule template.)
    """

    def __init__(self, predId, label, desc='', cardinality=-1):
        """

        :param predId: uri/id of the predicate
        :param label: name/label of the predicate
        :param desc:
        :param cardinality:
        """

        self.predId = predId
        self.label = label
        self.desc = desc
        self.ranges = set()
        self.cardinality = cardinality
        self.constraints = []
        self.policy = None

    def to_str(self):
        """Produces a textual representation of the predicate

        :return: text representation as predId(label)
        """

        return self.predId

    def to_json(self):
        """Produces a JSON representation of the predicate

        :return: json representation of the predicate
        """

        return {
            "predId": self.predId,
            'label': self.label,
            'desc': self.desc,
            'cardinality': self.cardinality,
            "ranges": [r for r in self.ranges],
            "constraints": [c for c in self.constraints]
        }

    def merge_with(self, other):
        if self.predId != other.predId:
            raise Exception("Cannot merge two different Predicates " + self.predId + ' and ' + other.predId)
        merged = Predicate(self.predId, self.label, self.desc, self.cardinality)
        if self.label is None or len(self.label) == 0:
            merged.label = other.label
        if self.desc is None or len(self.desc) == 0:
            merged.desc = other.desc
        if self.cardinality == -1:
            merged.cardinality = other.cardinality

        merged.ranges = set(list(self.ranges) + list(other.ranges))
        # TODO: merge constraints and polity (restriced first approach)

        return merged

    def addRanges(self, ranges):
        self.ranges.update(ranges)

    def __str__(self):
        return self.to_str()

    def __repr__(self):
        return self.to_str()

    def __eq__(self, other):
        return self.predId == other.predId

    def __hash__(self):
        return hash(self.predId)


class DataSourceType(Enum):
    SPARQL_ENDPOINT = "SPARQL_Endpoint"
    MONGODB = "MongoDB"
    NEO4J = "Neo4j"
    MYSQL = "MySQL"
    POSTGRES = "Postgres"

    SPARK_CSV = "SPARK_CSV"
    SPARK_TSV = "SPARK_TSV"
    SPARK_JSON = "SPARK_JSON"
    SPARK_XML = "SPARK_XML"

    HADOOP_CSV = "HADOOP_CSV"
    HADOOP_TSV = "HADOOP_TSV"
    HADOOP_JSON = "HADOOP_JSON"
    HADOOP_XML = "HADOOP_XML"

    REST_SERVICE = "REST_Service"

    LOCAL_CSV = "LOCAL_CSV"
    LOCAL_TSV = "LOCAL_TSV"
    LOCAL_JSON = "LOCAL_JSON"
    LOCAL_XML = "LOCAL_XML"
    LOCAL_RDF = "LOCAL_RDF"

    LOCAL_FOLDER = "LOCAL_FOLDER"
    SPARK_FOLDER = "SPARK_FOLDER"
    HADOOP_FOLDER = "HADOOP_FOLDER"

    CSV = "csv"
    TSV = "TSV"
    XML = "XML"
    JSON = "JSON"
    RDF = "RDF"

    def __repr__(self):
        return self.value

    def __str__(self):
        return self.value


metas = ['http://www.w3.org/ns/sparql-service-description',
         'http://www.openlinksw.com/schemas/virtrdf#',
         'http://www.w3.org/2000/01/rdf-schema#',
         'http://www.w3.org/1999/02/22-rdf-syntax-ns#',
         'http://www.w3.org/2002/07/owl#',
         'http://purl.org/dc/terms/Dataset',
         'http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/ProductType',
         'nodeID://']


class RDFMTExtractor:
    """ Extracts RDF-MTs from a sparql endpoint, or other sources

    ATM this class only implements the sparql endpoint sources
    """

    def __init__(self, sink_type='memory', path_to_sink='', params=None):
        """

        :param sink_type: sink to save/dump the molecule templates. default: memory
        :param path_to_sink: path to the sink. Either path to a json file or uri to sparql endpoint/mongodb collection.
        :param params: other parameters
        """

        self.sink_type = sink_type
        self.path_to_sink = path_to_sink
        self.params = params

    def get_molecules(self, datasource, typing_pred='a', collect_labels=False, collect_stats=False,
                      labeling_prop="http://www.w3.org/2000/01/rdf-schema#label", limit=-1, out_queue=None):
        endpoint = datasource.url

        if datasource.dstype != DataSourceType.SPARQL_ENDPOINT:
            return []
        rdfmts = []
        concepts = self.get_concepts(endpoint, collect_labels=collect_labels, collect_stats=collect_stats,
                                     labeling_prop=labeling_prop, typing_pred=typing_pred,
                                     limit=limit, out_queue=out_queue)
        for c in concepts:
            t = c['t']
            label = t
            if collect_labels:
                label = c['label']
            card = -1
            if 'card' in c:
                card = c['card']

            rdfmt = RDFMT(t, label, 'typed', cardinality=card)
            if 'subClassOf' in c:
                rdfmt.subClassOf = c['subClassOf']

            preds = self.get_predicates(endpoint, t, collect_labels=collect_labels, collect_stats=collect_stats,
                                        labeling_prop=labeling_prop, limit=limit, out_queue=out_queue)

            for p in preds:
                label = p['p']
                if collect_labels:
                    label = p['label']
                card = -1
                if 'card' in p:
                    card = p['card']
                pred = Predicate(p['p'], label, cardinality=card)

                ranges = self.get_predicate_ranges(endpoint, t, p['p'])
                pred.addRanges(ranges)
                rdfmt.addPredicate(pred)

            rdfmt.addDataSource(datasource)
            rdfmts.append(rdfmt)

        return rdfmts

    def get_concepts(self, endpoint, collect_labels=False, collect_stats=False,
                     labeling_prop="http://www.w3.org/2000/01/rdf-schema#label",
                     typing_pred='a', limit=-1, out_queue=None):
        """Entry point for extracting RDF-MTs of an endpoint.

            Extracts list of rdf:Class concepts from the endpoint

        :param endpoint:
        :param collect_labels: boolean value setting wheather to collect labels or not. default: False
        :param labeling_prop: if {collect_labels} is set `True`, then this labeling property will be used.
                        default: http://www.w3.org/2000/01/rdf-schema#label
        :param limit:
        :param typing_pred: typing predicate used in the endpoint.
                Can be any predicate uri, such as a or <http://www.w3.org/1999/02/22-rdf-syntax-ns#type>.
                 Should be in full uri format (not prefixed as rdf:type, wdt:P31, except 'a' which is the default)
                default: rdf:type or 'a'.
        :return:
        """
        query = "SELECT DISTINCT ?t WHERE{ ?s " + typing_pred + " ?t } "

        # if limit is not set, then set limit to 50, graceful request
        if limit == -1:
            limit = 50

        reslist, status = self._get_results_iter(query, endpoint, limit)

        # exclude some metadata classes
        reslist = [r for r in reslist if True not in [m in str(r['t']) for m in metas]]
        if collect_labels:
            reslist = self.get_labels(endpoint, reslist, 't', labeling_prop, 50)
        if collect_stats:
            reslist = self.get_cardinality(endpoint, reslist, 't')

        reslist = self.get_super_classes(endpoint, reslist, 't')

        return reslist

    def get_predicates(self, endpoint, rdfmt_id, collect_labels=False, collect_stats=False,
                       labeling_prop="http://www.w3.org/2000/01/rdf-schema#label",
                       limit=20, out_queue=None):
        """ Get list of predicates of a class rdfmt_id

        :param endpoint: endpoint
        :param rdfmt_id: RDF class Concept extracted from an endpoint
        :param collect_labels: boolean value setting wheather to collect labels or not. default: False
        :param labeling_prop: if {collect_labels} is set `True`, then this labeling property will be used.
                        default: http://www.w3.org/2000/01/rdf-schema#label
        :param limit:
        :return:
        """

        query = " SELECT DISTINCT ?p WHERE{ ?s a <" + rdfmt_id + ">. ?s ?p ?pt. } "

        if limit < 1:
            limit = 15
        reslist, status = self._get_results_iter(query, endpoint, limit)
        if status == -1:
            # fallback strategy - get predicates from randomly selected instances of {rdfmt_id}
            print(rdfmt_id, 'properties are not extracted properly. Falling back to randomly selected instances...')
            rand_inst_res = self._get_preds_of_sample_instances(endpoint, rdfmt_id)
            existingpreds = [r['p'] for r in reslist]
            for r in rand_inst_res:
                if r not in existingpreds:
                    reslist.append({'p': r})

        # collect labels if requested
        if collect_labels:
            reslist = self.get_labels(endpoint, reslist, 'p', labeling_prop, 5)
        if collect_stats:
            reslist = self.get_cardinality(endpoint, reslist, 'p')

        return reslist

    def get_predicate_ranges(self, endpoint, rdfmt_id, pred_id, limit=100):
        """get value ranges/rdfs ranges of the given predicate {pred_id}

        Ranges of a predicate {pred_id} can be obtained in two ways:
            1. by using rdfs:range property, if defined by the RDFS, and
            2. by checking the rdf:type/wdt:P31 of the object values associated to the given {rdfmt_id} instance and
               its predicate {pred_id}

        :param endpoint: url
        :param rdfmt_id: rdfmt
        :param pred_id: predicate
        :param limit: int, default: 100
        :return: list of ranges
        """

        ranges = self._get_rdfs_ranges(endpoint, pred_id)
        ranges.extend(self._find_instance_range(endpoint, rdfmt_id, pred_id))

        return ranges

    def _get_rdfs_ranges(self, endpoint, pred_id, limit=-1):

        RDFS_RANGES = " SELECT DISTINCT ?range  WHERE{ <" + pred_id + "> <http://www.w3.org/2000/01/rdf-schema#range> ?range. }"

        if limit == -1:
            limit = 50

        reslist, status = self._get_results_iter(RDFS_RANGES, endpoint, limit)

        ranges = []

        for r in reslist:
            skip = False
            for m in metas:
                if m in r['range']:
                    skip = True
                    break
            if not skip:
                ranges.append(r['range'])

        return ranges

    def _find_instance_range(self, endpoint, rdfmt_id, pred_id, limit=-1):
        """extract ranges of a predicate {pred_id} associated to RDF-MT {rdfmt_id}

        :param endpoint:
        :param rdfmt_id:
        :param pred_id:
        :param limit:
        :return:
        """
        INSTANCE_RANGES = " SELECT DISTINCT ?r WHERE{ ?s a <" + rdfmt_id + ">. ?s <" + pred_id + "> ?pt.  ?pt a ?r  } "
        INSTANCE_RANGES_DType = " SELECT DISTINCT datatype(?pt) as ?r WHERE{ ?s a <" + rdfmt_id + ">. ?s <" + pred_id + "> ?pt. } "

        if limit == -1:
            limit = 50

        reslist, status = self._get_results_iter(INSTANCE_RANGES, endpoint, limit)
        reslist2, status2 = self._get_results_iter(INSTANCE_RANGES_DType, endpoint, limit)
        reslist2 = [r for r in reslist2 if len(r) > 0]
        if len(reslist2) > 0:
            reslist.extend(reslist2)

        ranges = []

        for r in reslist:
            skip = False
            for m in metas:
                if 'r' in r and m in r['r']:
                    skip = True
                    break
            if not skip:
                if 'r' in r:
                    ranges.append(r['r'])

        return ranges

    def _get_results_iter(self, query, endpoint, limit, max_rows=-1, out_queue=None):
        offset = 0
        reslist = []
        status = 0

        while True:
            query_copy = query + " LIMIT " + str(limit) + ( " OFFSET " + str(offset) if offset > 0 else '')
            res, card = contact_sparql_endpoint(query_copy, endpoint)

            # in case source fails because of the data/row limit, try again up to limit = 1
            if card == -2:
                limit = limit // 2
                if limit < 1:
                    status = -1
                    break
                continue

            # if results are returned from the endpoint, append them to the results list
            if card > 0:
                reslist.extend(res)

                # if output queue is given, then put each non-metadata classes to the queue
                if out_queue is not None:
                    for r in res:
                        if True not in [m in str(r['t']) for m in metas]:
                            out_queue.put(r)

            # if number of rows returned are less than the requested limit, then we are done
            if card < limit or (max_rows > 0 and len(reslist) >= max_rows):
                break
            offset += limit

        return reslist, status

    def _get_preds_of_sample_instances(self, endpoint, rdfmt_id, limit=50):

        """get a union of predicates from the first 100 subjects returned

        :param endpoint: endpoint
        :param rdfmt_id: rdf class concept of and endpoint
        :param limit:
        :return:
        """

        query = " SELECT DISTINCT ?s WHERE{ ?s a <" + rdfmt_id + ">. } "

        if limit < 1:
            limit = 50

        reslist, status = self._get_results_iter(query, endpoint, limit, max_rows=100)

        batches = []
        for i in range(len(reslist)):
            inst = reslist[i]
            if len(batches) == 10 or i + 1 == len(batches):
                inst_res = self._get_preds_of_instances(endpoint, batches)
                inst_res = [r['p'] for r in inst_res]
                reslist.extend(inst_res)
                reslist = list(set(reslist))
                batches = []
            else:
                batches.append(inst['s'])

        return reslist

    def _get_preds_of_instances(self, endpoint, insts, limit=100):
        """get union of predicates from the given set of instances, {insts}

        :param endpoint: url
        :param insts: list of instances
        :param limit: limit, default= 100
        :return:
        """

        unions = ["{ <" + inst + "> ?p ?pt } " for inst in insts]
        query = " SELECT DISTINCT ?p WHERE{ " + " UNION ".join(unions) + " } "
        reslist = []
        if limit < 1:
            limit = 100
        offset = 0

        while True:
            query_copy = query + " LIMIT " + str(limit) + " OFFSET " + str(offset)
            res, card = contact_sparql_endpoint(query_copy, endpoint)

            # in case source fails because of the data/row limit, try again up to limit = 1
            if card == -2:
                limit = limit // 2
                if limit < 1:
                    break
                continue

            # if results are returned from the endpoint, append them to the results list
            if card > 0:
                reslist.extend(res)

            # if number of rows returned are less than the requested limit, then we are done
            if card < limit:
                break

            offset += limit

        return reslist

    def get_labels(self, endpoint, ids, key, labeling_prop, limit):
        """Collect labels for the given uris in a dictionary {ids}

        :param endpoint: sparql endpoint
        :param ids: list of dict values
        :param key: key to access the rdfmt_id or pred_id
        :param labeling_prop:
        :param limit:

        :return: updated list {ids} with additional element 'label'
        """
        result = []
        batches = []
        for i in range(0, len(ids), 10):
            batches = ids[i: i+10] if i+10 < len(ids) else ids[i:]

            ggp = ["{ <" + batches[j][key] + ">  <" + labeling_prop + "> ?l" + str(
                j) + " . filter (lcase(lang(?l" + str(j) + ")) = 'en')}"
                   for j in range(len(batches))]

            query = "SELECT DISTINCT * WHERE{" + " UNION ".join(ggp) + "} "
            reslist, status = self._get_results_iter(query, endpoint, limit)

            # set the default label, i.e., same as its id
            for b in batches:
                b['label'] = b[key]

            for r in reslist:
                for j in range(len(batches)):
                    if len(r['l' + str(j)]) > 0:
                        batches[j]['label'] = r['l' + str(j)]
            result.extend(batches)
            # reset batches list to empty


        return result

    def get_super_classes(self, endpoint, ids, key, limit=15):
        """Collect all superclasses of the given RDF-MT {rdfmt_id}

        :param endpoint:
        :param rdfmt_id:
        :param limit:
        :return:
        """
        results = []
        for t in ids:
            rdfmt_id = t[key]
            # uses path query to get all superclasses, since subClassOf property is transitive
            query = "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> " \
                    " SELECT DISTINCT * WHERE { <" + rdfmt_id + "> rdfs:subClassOf* ?sc }"
            # if limit is not set, then set limit to 50, graceful request
            if limit == -1:
                limit = 15

            reslist, status = self._get_results_iter(query, endpoint, limit)

            # exclude some metadata classes
            reslist = [r for r in reslist if True not in [m in str(r['sc']) for m in metas]]
            t['subClassOf'] = reslist
            results.append(t)

        return results

    def get_cardinality(self, endpoint, ids, key):
        """collect cardinality of the given RDF-MT {rdfmt_id}

        :param endpoint:
        :param ids:
        :param key:
        :return:
        """
        results = []
        for t in ids:
            rdfmt_id = t[key]

            query = " SELECT COUNT(DISTINCT ?s) as ?card WHERE {?s a <" + rdfmt_id + "> }"

            reslist, status = self._get_results_iter(query, endpoint, 10)

            # set cardinality as unknown (-1)
            card = -1

            if reslist is not None and len(reslist) > 0:
                card = reslist[0]['card']

            t['card'] = card
            results.append(t)

        return results

