from neo4j.v1 import GraphDatabase

class HPCUserDatabase(object):

    def __init__(self, uri, user, password):
        self._driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self._driver.close()

    def print_greeting(self, message):
        with self._driver.session() as session:
            greeting = session.write_transaction(self._create_and_return_greeting, message)
            print(greeting)

    def print_user(self, name):
        with self._driver.session() as session:
            user = session.write_transaction(self._create_user, name)
            print(user)

    def delete_database(self):
        with self._driver.session() as session:
            session.run("MATCH(n) DETACH DELETE n")
            print("database deleted")

    def load_slurm_data(self):
        pass

    def load_data(self):
        with self._driver.session() as session:
             session.run("USING PERIODIC COMMIT LOAD CSV WITH HEADERS FROM "
                            "'http://people.cs.ksu.edu/~happystep/HPC/humanDate+survey.csv' AS csvLine "
                            "FIELDTERMINATOR ',' CREATE (u:User {"
                            "qname: csvLine.qname, hostname: csvLine.hostname,group: csvLine.group,"
                         "owner: csvLine.owner,job_name: csvLine.job_name,job_number: toInteger(csvLine.job_number),"
                         "submission_time: csvLine.submission_time,start_time: csvLine.start_time,end_time: "
                         "csvLine.end_time,failed: toInteger(csvLine.failed),exit_status: toInteger("
                         "csvLine.exit_status),"
                         "ru_wallclock: toInteger(csvLine.ru_wallclock),ru_utime: toFloat(csvLine.ru_utime),ru_stime: "
                         "toFloat(csvLine.ru_stime),ru_maxrss: toFloat(csvLine.ru_maxrss),ru_ixrss: "
                         "toInteger(csvLine.ru_ixrss),"
                         "ru_ismrss: toInteger(csvLine.ru_ismrss),ru_idrss: toInteger(csvLine.ru_idrss),ru_isrss: "
                         "toInteger(csvLine.ru_isrss),"
                         "ru_minflt: toInteger(csvLine.ru_minflt),ru_majflt: toInteger(csvLine.ru_majflt),ru_nswap: "
                         "toInteger(csvLine.ru_nswap),"
                         "ru_inblock: toFloat(csvLine.ru_inblock),ru_oublock: toInteger(csvLine.ru_oublock),ru_msgsnd: "
                         "toInteger(csvLine.ru_msgsnd),ru_msgrcv: toInteger(csvLine.ru_msgrcv),ru_nsignals: "
                         "toInteger(csvLine.ru_nsignals),ru_nvcsw: toInteger(csvLine.ru_nvcsw),ru_nivcsw: "
                         "toInteger(csvLine.ru_nivcsw),project: csvLine.project,granted_pe: csvLine.granted_pe,"
                         "slots: toInteger(csvLine.slots),task_number: toInteger(csvLine.task_number),"
                         "cpu: toFloat(csvLine.cpu),mem: toFloat(csvLine.mem),io: toFloat(csvLine.io),category: "
                         "csvLine.category,"
                         "iow: toFloat(csvLine.iow),pe_taskid: csvLine.pe_taskid,maxvmem: toFloat(csvLine.maxvmem),"
                         "dep: csvLine.dep,university: csvLine.university,reqTime: toFloat(csvLine.reqTime),"
                         "reqMem: toFloat(csvLine.reqMem),people: csvLine.people,cumaCPU: toFloat(csvLine.cumaCPU),"
                         "cumaReqmem: toFloat(csvLine.cumaReqmem),minCPU: toFloat(csvLine.minCPU),minReqMem: "
                         "toFloat(csvLine.minReqMem),maxCPU: toFloat(csvLine.maxCPU),maxReqMem: "
                         "toFloat(csvLine.maxReqMem),stdCPU: toFloat(csvLine.stdCPU),stdReqMem: "
                         "toFloat(csvLine.stdReqMem),cntUser: toFloat(csvLine.cntUser),killrate: "
                         "toFloat(csvLine.killrate),q5_experience: toInteger(csvLine.q5_experience),q6_proficiency: "
                         "toInteger(csvLine.q6_proficiency),q7_training: toInteger(csvLine.q7_training) })")
        print("database loaded")

    def create_index(self):
        with self._driver.session() as session:
            session.run("CREATE INDEX ON :USER (qname) ;CREATE INDEX ON :USER (hostname) ;CREATE INDEX ON :USER (group) ;CREATE INDEX ON :USER (owner) ;CREATE INDEX ON :USER (job_name) ;CREATE INDEX ON :USER (job_number) ;CREATE INDEX ON :USER (submission_time) ;CREATE INDEX ON :USER (start_time) ;CREATE INDEX ON :USER (end_time) ;CREATE INDEX ON :USER (failed) ;CREATE INDEX ON :USER (exit_status) ;CREATE INDEX ON :USER (ru_wallclock) ;CREATE INDEX ON :USER (ru_utime) ;CREATE INDEX ON :USER (ru_stime) ;CREATE INDEX ON :USER (ru_maxrss) ;CREATE INDEX ON :USER (ru_ixrss) ;CREATE INDEX ON :USER (ru_ismrss) ;CREATE INDEX ON :USER (ru_idrss) ;CREATE INDEX ON :USER (ru_isrss) ;CREATE INDEX ON :USER (ru_minflt) ;CREATE INDEX ON :USER (ru_majflt) ;CREATE INDEX ON :USER (ru_nswap) ;CREATE INDEX ON :USER (ru_inblock) ;CREATE INDEX ON :USER (ru_oublock) ;CREATE INDEX ON :USER (ru_msgsnd) ;CREATE INDEX ON :USER (ru_msgrcv) ;CREATE INDEX ON :USER (ru_nsignals) ;CREATE INDEX ON :USER (ru_nvcsw) ;CREATE INDEX ON :USER (ru_nivcsw) ;CREATE INDEX ON :USER (project) ;CREATE INDEX ON :USER (granted_pe) ;CREATE INDEX ON :USER (slots) ;CREATE INDEX ON :USER (task_number) ;CREATE INDEX ON :USER (cpu) ;CREATE INDEX ON :USER (mem) ;CREATE INDEX ON :USER (io) ;CREATE INDEX ON :USER (category) ;CREATE INDEX ON :USER (iow) ;CREATE INDEX ON :USER (pe_taskid) ;CREATE INDEX ON :USER (maxvmem) ;CREATE INDEX ON :USER (dep) ;CREATE INDEX ON :USER (university) ;CREATE INDEX ON :USER (reqTime) ;CREATE INDEX ON :USER (reqMem) ;CREATE INDEX ON :USER (people) ;CREATE INDEX ON :USER (cumaCPU) ;CREATE INDEX ON :USER (cumaReqmem) ;CREATE INDEX ON :USER (minCPU) ;CREATE INDEX ON :USER (minReqMem) ;CREATE INDEX ON :USER (maxCPU) ;CREATE INDEX ON :USER (maxReqMem) ;CREATE INDEX ON :USER (stdCPU) ;CREATE INDEX ON :USER (stdReqMem) ;CREATE INDEX ON :USER (cntUser) ;CREATE INDEX ON :USER (killrate) ;CREATE INDEX ON :USER (q5_experience) ;CREATE INDEX ON :USER (q6_proficiency) ;CREATE INDEX ON :USER (q7_training)")


    def load_test_data(self):
        with self._driver.session() as session:
            session.run("USING PERIODIC COMMIT LOAD CSV WITH HEADERS FROM "
             "'http://people.cs.ksu.edu/~happystep/HPC/test_hpc.csv' AS csvLine "
             "FIELDTERMINATOR ',' CREATE (u:User {"
             "qname: csvLine.qname, hostname: csvLine.hostname,group: csvLine.group,"
             "owner: csvLine.owner,job_name: csvLine.job_name,job_number: toInteger(csvLine.job_number),"
             "submission_time: csvLine.submission_time,start_time: csvLine.start_time,end_time: "
             "csvLine.end_time,failed: toInteger(csvLine.failed),exit_status: toInteger("
             "csvLine.exit_status),"
             "ru_wallclock: toInteger(csvLine.ru_wallclock),ru_utime: toFloat(csvLine.ru_utime),ru_stime: "
             "toFloat(csvLine.ru_stime),ru_maxrss: toFloat(csvLine.ru_maxrss),ru_ixrss: "
             "toInteger(csvLine.ru_ixrss),"
             "ru_ismrss: toInteger(csvLine.ru_ismrss),ru_idrss: toInteger(csvLine.ru_idrss),ru_isrss: "
             "toInteger(csvLine.ru_isrss),"
             "ru_minflt: toInteger(csvLine.ru_minflt),ru_majflt: toInteger(csvLine.ru_majflt),ru_nswap: "
             "toInteger(csvLine.ru_nswap),"
             "ru_inblock: toFloat(csvLine.ru_inblock),ru_oublock: toInteger(csvLine.ru_oublock),ru_msgsnd: "
             "toInteger(csvLine.ru_msgsnd),ru_msgrcv: toInteger(csvLine.ru_msgrcv),ru_nsignals: "
             "toInteger(csvLine.ru_nsignals),ru_nvcsw: toInteger(csvLine.ru_nvcsw),ru_nivcsw: "
             "toInteger(csvLine.ru_nivcsw),project: csvLine.project,granted_pe: csvLine.granted_pe,"
             "slots: toInteger(csvLine.slots),task_number: toInteger(csvLine.task_number),"
             "cpu: toFloat(csvLine.cpu),mem: toFloat(csvLine.mem),io: toFloat(csvLine.io),category: "
             "csvLine.category,"
             "iow: toFloat(csvLine.iow),pe_taskid: csvLine.pe_taskid,maxvmem: toFloat(csvLine.maxvmem),"
             "dep: csvLine.dep,university: csvLine.university,reqTime: toFloat(csvLine.reqTime),"
             "reqMem: toFloat(csvLine.reqMem),people: csvLine.people,cumaCPU: toFloat(csvLine.cumaCPU),"
             "cumaReqmem: toFloat(csvLine.cumaReqmem),minCPU: toFloat(csvLine.minCPU),minReqMem: "
             "toFloat(csvLine.minReqMem),maxCPU: toFloat(csvLine.maxCPU),maxReqMem: "
             "toFloat(csvLine.maxReqMem),stdCPU: toFloat(csvLine.stdCPU),stdReqMem: "
             "toFloat(csvLine.stdReqMem),cntUser: toFloat(csvLine.cntUser),killrate: "
             "toFloat(csvLine.killrate),q5_experience: toInteger(csvLine.q5_experience),q6_proficiency: "
             "toInteger(csvLine.q6_proficiency),q7_training: toInteger(csvLine.q7_training) })")

    @staticmethod
    def _create_and_return_greeting(tx, message):
        result = tx.run("CREATE (a:Greeting) "
                        "SET a.message = $message "
                        "RETURN a.message + ', from node ' + id(a)", message=message)
        return result.single()[0]

    @staticmethod
    def _create_user(tx, name):
        result = tx.run("CREATE (u:User {name: $name})"
                         "return u.name" , name=name)
        return result.single()[0]

    @staticmethod
    def _load_data_from_csv(tx):
        result = tx.run("USING PERIODIC COMMIT LOAD CSV WITH HEADERS FROM "
                        "'http://people.cs.ksu.edu/~happystep/HPC/humanDate+survey.csv' AS csvLine "
                        "FIELDTERMINATOR ',' CREATE (u:User {"
                        "name: csvLine.owner, job_number: toInteger(csvLine.job_number) })")
        return "pass" #result.consume also works?
        # https://neo4j.com/docs/api/java-driver/current/org/neo4j/driver/v1/StatementResult.html

    @staticmethod
    def _load_test_data_from_csv(tx):
        result = tx.run("USING PERIODIC COMMIT LOAD CSV WITH HEADERS FROM "
                            "'http://people.cs.ksu.edu/~happystep/HPC/test_hpc.csv' AS csvLine "
                            "FIELDTERMINATOR ',' CREATE (u:User {"
                            "qname: csvLine.qname, hostname: csvLine.hostname,group: csvLine.group,"
                         "owner: csvLine.owner,job_name: csvLine.job_name,job_number: toInteger(csvLine.job_number),"
                         "submission_time: csvLine.submission_time,start_time: csvLine.start_time,end_time: "
                         "csvLine.end_time,failed: toInteger(csvLine.failed),exit_status: toInteger("
                         "csvLine.exit_status),"
                         "ru_wallclock: toInteger(csvLine.ru_wallclock),ru_utime: toFloat(csvLine.ru_utime),ru_stime: "
                         "toFloat(csvLine.ru_stime),ru_maxrss: toFloat(csvLine.ru_maxrss),ru_ixrss: "
                         "toInteger(csvLine.ru_ixrss),"
                         "ru_ismrss: toInteger(csvLine.ru_ismrss),ru_idrss: toInteger(csvLine.ru_idrss),ru_isrss: "
                         "toInteger(csvLine.ru_isrss),"
                         "ru_minflt: toInteger(csvLine.ru_minflt),ru_majflt: toInteger(csvLine.ru_majflt),ru_nswap: "
                         "toInteger(csvLine.ru_nswap),"
                         "ru_inblock: toFloat(csvLine.ru_inblock),ru_oublock: toInteger(csvLine.ru_oublock),ru_msgsnd: "
                         "toInteger(csvLine.ru_msgsnd),ru_msgrcv: toInteger(csvLine.ru_msgrcv),ru_nsignals: "
                         "toInteger(csvLine.ru_nsignals),ru_nvcsw: toInteger(csvLine.ru_nvcsw),ru_nivcsw: "
                         "toInteger(csvLine.ru_nivcsw),project: csvLine.project,granted_pe: csvLine.granted_pe,"
                         "slots: toInteger(csvLine.slots),task_number: toInteger(csvLine.task_number),"
                         "cpu: toFloat(csvLine.cpu),mem: toFloat(csvLine.mem),io: toFloat(csvLine.io),category: "
                         "csvLine.category,"
                         "iow: toFloat(csvLine.iow),pe_taskid: csvLine.pe_taskid,maxvmem: toFloat(csvLine.maxvmem),"
                         "dep: csvLine.dep,university: csvLine.university,reqTime: toFloat(csvLine.reqTime),"
                         "reqMem: toFloat(csvLine.reqMem),people: csvLine.people,cumaCPU: toFloat(csvLine.cumaCPU),"
                         "cumaReqmem: toFloat(csvLine.cumaReqmem),minCPU: toFloat(csvLine.minCPU),minReqMem: "
                         "toFloat(csvLine.minReqMem),maxCPU: toFloat(csvLine.maxCPU),maxReqMem: "
                         "toFloat(csvLine.maxReqMem),stdCPU: toFloat(csvLine.stdCPU),stdReqMem: "
                         "toFloat(csvLine.stdReqMem),cntUser: toFloat(csvLine.cntUser),killrate: "
                         "toFloat(csvLine.killrate),q5_experience: toInteger(csvLine.q5_experience),q6_proficiency: "
                         "toInteger(csvLine.q6_proficiency),q7_training: toInteger(csvLine.q7_training) })")
        return "pass"  # result.consume also works?
        # https://neo4j.com/docs/api/java-driver/current/org/neo4j/driver/v1/StatementResult.html
