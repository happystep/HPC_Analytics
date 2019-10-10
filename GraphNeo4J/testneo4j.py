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

    def aggregate_users_create_relationships(self):
        with self._driver.session() as session:
            result = session.run("MATCH (a:User),(b:User) WHERE a.Name = b.User CREATE (a)-[r:Person]->(b) RETURN r")
        return(result)

    def query_small_set(self):
        with self._driver.session() as session:
            result = session.run("MATCH (n) OPTIONAL MATCH (n)-[r]->() RETURN n, r limit 50")
        return(result)


    def query_full_set(self):
        with self._driver.session() as session:
            result = session.run("MATCH (n) OPTIONAL MATCH (n)-[r]->() RETURN n, r")
            print(result)

    def show_data(self):
        with self._driver.session() as session:
            result = session.run("MATCH(n) return n")
            print(result)

    def load_slurm_data(self):
        with self._driver.session() as session:
            session.run("USING PERIODIC COMMIT LOAD CSV WITH HEADERS FROM "
                        "'http://people.cs.ksu.edu/~happystep/HPC/slurmUserBasedMemory.csv' AS csvLine "
                        "FIELDTERMINATOR ',' CREATE (u:User {"
                        "Account: csvLine.Account, AllocCPUS: toFloat(csvLine.AllocCPUS), AllocNodes: toFloat(csvLine.AllocNodes),"
                        "AllocTRES: csvLine.AllocTRES, AssocID: toFloat(csvLine.AssocID), CPUTime: csvLine.CPUTime,"
                        "CPUTimeRAW: toFloat(csvLine.CPUTimeRAW), DerivedExitCode: csvLine.DerivedExitCode, Elapsed: "
                        "csvLine.Elapsed, ElapsedRAW: toFloat(csvLine.ElapsedRAW), Eligible: "
                        "csvLine.Eligible,"
                        "End: csvLine.End, ExitCode: csvLine.ExitCode, GID: "
                        "toFloat(csvLine.GID), JobID: csvLine.JobID, JobIDRaw: "
                        "toFloat(csvLine.JobIDRaw),"
                        "JobName: csvLine.JobName, NCPUS: toFloat(csvLine.NCPUS), NNodes: "
                        "toFloat(csvLine.NNodes),"
                        "NodeList: csvLine.NodeList, Priority: toFloat(csvLine.Priority), Partition: "
                        "csvLine.Partition,"
                        "ReqCPUS: toFloat(csvLine.ReqCPUS), ReqMem: csvLine.ReqMem, ReqNodes: "
                        "toFloat(csvLine.ReqNodes), ReqTRES: csvLine.ReqTRES, Reserved: "
                        "csvLine.Reserved, ResvCPU: csvLine.ResvCPU, ResvCPURAW: "
                        "toFloat(csvLine.ResvCPURAW), Start: csvLine.Start, State: csvLine.State,"
                        "Submit: csvLine.Submit, SystemCPU: csvLine.SystemCPU,"
                        "Timelimit: csvLine.Timelimit, TimelimitRaw: toFloat(csvLine.TimelimitRaw), TotalCPU: csvLine.TotalCPU, UID: "
                        "toFloat(csvLine.UID),"
                        "User: csvLine.User, UserCPU: csvLine.UserCPU, WCKeyID: toFloat(csvLine.WCKeyID),"
                        "WorkDir: csvLine.WorkDir, ReservedRAW: toFloat(csvLine.ReservedRAW), TotalCPURAW: toFloat(csvLine.TotalCPURAW),"
                        "SystemCPURAW: toFloat(csvLine.SystemCPURAW), UserCPURAW: toFloat(csvLine.UserCPURAW), billing: csvLine.billing,"
                        "failed: toFloat(csvLine.failed), SubmitRAW: toFloat(csvLine.SubmitRAW), StartRAW: "
                        "toFloat(csvLine.StartRAW), EligibleRAW: toFloat(csvLine.EligibleRAW), EndRAW: "
                        "toFloat(csvLine.EndRAW), AllocMem: toFloat(csvLine.AllocMem), Dep: "
                        "csvLine.Dep, University: csvLine.University, Role: "
                        "csvLine.Role, AllocMemTRES: toFloat(csvLine.AllocMemTRES), ReqMemTRES: "
                        "toFloat(csvLine.ReqMemTRES)})")
        print("database loaded")

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

    def create_slurm_index(self):
        with self._driver.session() as session:
            session.run(
                "CREATE INDEX ON :USER (Account ); CREATE INDEX ON :USER (AllocCPUS ); CREATE INDEX ON :USER (AllocNodes ); CREATE INDEX ON :USER (AllocTRES ); CREATE INDEX ON :USER (AssocID ); CREATE INDEX ON :USER (CPUTime ); CREATE INDEX ON :USER (CPUTimeRAW ); CREATE INDEX ON :USER (DerivedExitCode ); CREATE INDEX ON :USER (Elapsed ); CREATE INDEX ON :USER (ElapsedRaw ); CREATE INDEX ON :USER (Eligible ); CREATE INDEX ON :USER (End ); CREATE INDEX ON :USER (ExitCode ); CREATE INDEX ON :USER (GID ); CREATE INDEX ON :USER (JobID ); CREATE INDEX ON :USER (JobIDRaw ); CREATE INDEX ON :USER (JobName ); CREATE INDEX ON :USER (NCPUS ); CREATE INDEX ON :USER (NNodes ); CREATE INDEX ON :USER (NodeList ); CREATE INDEX ON :USER (Priority ); CREATE INDEX ON :USER (Partition ); CREATE INDEX ON :USER (ReqCPUS ); CREATE INDEX ON :USER (ReqMem ); CREATE INDEX ON :USER (ReqNodes ); CREATE INDEX ON :USER (ReqTRES ); CREATE INDEX ON :USER (Reserved ); CREATE INDEX ON :USER (ResvCPU ); CREATE INDEX ON :USER (ResvCPURAW ); CREATE INDEX ON :USER (Start ); CREATE INDEX ON :USER (State ); CREATE INDEX ON :USER (Submit ); CREATE INDEX ON :USER (SystemCPU ); CREATE INDEX ON :USER (Timelimit ); CREATE INDEX ON :USER (TimelimitRaw ); CREATE INDEX ON :USER (TotalCPU ); CREATE INDEX ON :USER (UID ); CREATE INDEX ON :USER (User ); CREATE INDEX ON :USER (UserCPU ); CREATE INDEX ON :USER (WCKeyID ); CREATE INDEX ON :USER (WorkDir ); CREATE INDEX ON :USER (ReservedRAW ); CREATE INDEX ON :USER (TotalCPURAW ); CREATE INDEX ON :USER (SystemCPURAW ); CREATE INDEX ON :USER (UserCPURAW ); CREATE INDEX ON :USER (billing ); CREATE INDEX ON :USER (failed ); CREATE INDEX ON :USER (SubmitRAW ); CREATE INDEX ON :USER (StartRAW ); CREATE INDEX ON :USER (EligibleRAW ); CREATE INDEX ON :USER (EndRAW ); CREATE INDEX ON :USER (AllocMem ); CREATE INDEX ON :USER (Dep ); CREATE INDEX ON :USER (University ); CREATE INDEX ON :USER (Role ); CREATE INDEX ON :USER (AllocMemTRES ); CREATE INDEX ON :USER (ReqMemTRES )"
            )

    def create_sge_index(self):
        with self._driver.session() as session:
            session.run(
                "CREATE INDEX ON :USER (qname) ;CREATE INDEX ON :USER (hostname) ;CREATE INDEX ON :USER (group) ;CREATE INDEX ON :USER (owner) ;CREATE INDEX ON :USER (job_name) ;CREATE INDEX ON :USER (job_number) ;CREATE INDEX ON :USER (submission_time) ;CREATE INDEX ON :USER (start_time) ;CREATE INDEX ON :USER (end_time) ;CREATE INDEX ON :USER (failed) ;CREATE INDEX ON :USER (exit_status) ;CREATE INDEX ON :USER (ru_wallclock) ;CREATE INDEX ON :USER (ru_utime) ;CREATE INDEX ON :USER (ru_stime) ;CREATE INDEX ON :USER (ru_maxrss) ;CREATE INDEX ON :USER (ru_ixrss) ;CREATE INDEX ON :USER (ru_ismrss) ;CREATE INDEX ON :USER (ru_idrss) ;CREATE INDEX ON :USER (ru_isrss) ;CREATE INDEX ON :USER (ru_minflt) ;CREATE INDEX ON :USER (ru_majflt) ;CREATE INDEX ON :USER (ru_nswap) ;CREATE INDEX ON :USER (ru_inblock) ;CREATE INDEX ON :USER (ru_oublock) ;CREATE INDEX ON :USER (ru_msgsnd) ;CREATE INDEX ON :USER (ru_msgrcv) ;CREATE INDEX ON :USER (ru_nsignals) ;CREATE INDEX ON :USER (ru_nvcsw) ;CREATE INDEX ON :USER (ru_nivcsw) ;CREATE INDEX ON :USER (project) ;CREATE INDEX ON :USER (granted_pe) ;CREATE INDEX ON :USER (slots) ;CREATE INDEX ON :USER (task_number) ;CREATE INDEX ON :USER (cpu) ;CREATE INDEX ON :USER (mem) ;CREATE INDEX ON :USER (io) ;CREATE INDEX ON :USER (category) ;CREATE INDEX ON :USER (iow) ;CREATE INDEX ON :USER (pe_taskid) ;CREATE INDEX ON :USER (maxvmem) ;CREATE INDEX ON :USER (dep) ;CREATE INDEX ON :USER (university) ;CREATE INDEX ON :USER (reqTime) ;CREATE INDEX ON :USER (reqMem) ;CREATE INDEX ON :USER (people) ;CREATE INDEX ON :USER (cumaCPU) ;CREATE INDEX ON :USER (cumaReqmem) ;CREATE INDEX ON :USER (minCPU) ;CREATE INDEX ON :USER (minReqMem) ;CREATE INDEX ON :USER (maxCPU) ;CREATE INDEX ON :USER (maxReqMem) ;CREATE INDEX ON :USER (stdCPU) ;CREATE INDEX ON :USER (stdReqMem) ;CREATE INDEX ON :USER (cntUser) ;CREATE INDEX ON :USER (killrate) ;CREATE INDEX ON :USER (q5_experience) ;CREATE INDEX ON :USER (q6_proficiency) ;CREATE INDEX ON :USER (q7_training)")

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
                        "return u.name", name=name)
        return result.single()[0]

    @staticmethod
    def _load_data_from_csv(tx):
        result = tx.run("USING PERIODIC COMMIT LOAD CSV WITH HEADERS FROM "
                        "'http://people.cs.ksu.edu/~happystep/HPC/humanDate+survey.csv' AS csvLine "
                        "FIELDTERMINATOR ',' CREATE (u:User {"
                        "name: csvLine.owner, job_number: toInteger(csvLine.job_number) })")
        return "pass"  # result.consume also works?
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
