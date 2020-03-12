from neo4j.v1 import GraphDatabase


class HPCJobDatabase(object):

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

    def delete_user(self, q):
        with self._driver.session() as session:
            session.run("MATCH(n) where n.User = '" + q + "' detach delete n")
            print("user " + q + " deleted")

    def delete_database(self):
        with self._driver.session() as session:
            session.run("MATCH(n) DETACH DELETE n")
            print("database deleted")

    def user_count(self):
        list_of_dictionaries = []
        with self._driver.session() as session:
            result = session.run("MATCH (n) RETURN DISTINCT n.User, count(*) as freq order by freq DESC;")
            for i in result.records():
                list_of_dictionaries.append(i.data())
        return list_of_dictionaries

# CREATE (a)-[r:RELTYPE { name: a.name + '<->' + b.name }]->(b)
    def users_create_relationships(self, q): #q is the stirng of the user
        with self._driver.session() as session:
            result = session.run("MATCH (a:User),(b:User) WHERE a.User = '" + q + "' AND  b.User = '" + q + "' CREATE (a)-[r:Person { name: a.User + '<->' + b.User }]->(b) RETURN type(r)")
        print('relationships loaded')
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

    def load_slurm_sample_data(self):
        with self._driver.session() as session:
            session.run("USING PERIODIC COMMIT LOAD CSV WITH HEADERS FROM "
                        "'http://people.cs.ksu.edu/~happystep/HPC/slurm_sample_cleaned.csv' AS csvLine "
                        "FIELDTERMINATOR ',' CREATE (j:Job {"
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

    def load_slurm_data(self):
        with self._driver.session() as session:
            session.run("USING PERIODIC COMMIT LOAD CSV WITH HEADERS FROM "
                        "'http://people.cs.ksu.edu/~happystep/HPC/slurmUserBasedMemory.csv' AS csvLine "
                        "FIELDTERMINATOR ',' CREATE (j:Job {"
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
                        "FIELDTERMINATOR ',' CREATE (j:Job {"
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
                "CREATE INDEX ON :JOB (Account ); CREATE INDEX ON :JOB (AllocCPUS ); CREATE INDEX ON :JOB (AllocNodes ); CREATE INDEX ON :JOB (AllocTRES ); CREATE INDEX ON :JOB (AssocID ); CREATE INDEX ON :JOB (CPUTime ); CREATE INDEX ON :JOB (CPUTimeRAW ); CREATE INDEX ON :JOB (DerivedExitCode ); CREATE INDEX ON :JOB (Elapsed ); CREATE INDEX ON :JOB (ElapsedRaw ); CREATE INDEX ON :JOB (Eligible ); CREATE INDEX ON :JOB (End ); CREATE INDEX ON :JOB (ExitCode ); CREATE INDEX ON :JOB (GID ); CREATE INDEX ON :JOB (JobID ); CREATE INDEX ON :JOB (JobIDRaw ); CREATE INDEX ON :JOB (JobName ); CREATE INDEX ON :JOB (NCPUS ); CREATE INDEX ON :JOB (NNodes ); CREATE INDEX ON :JOB (NodeList ); CREATE INDEX ON :JOB (Priority ); CREATE INDEX ON :JOB (Partition ); CREATE INDEX ON :JOB (ReqCPUS ); CREATE INDEX ON :JOB (ReqMem ); CREATE INDEX ON :JOB (ReqNodes ); CREATE INDEX ON :JOB (ReqTRES ); CREATE INDEX ON :JOB (Reserved ); CREATE INDEX ON :JOB (ResvCPU ); CREATE INDEX ON :JOB (ResvCPURAW ); CREATE INDEX ON :JOB (Start ); CREATE INDEX ON :JOB (State ); CREATE INDEX ON :JOB (Submit ); CREATE INDEX ON :JOB (SystemCPU ); CREATE INDEX ON :JOB (Timelimit ); CREATE INDEX ON :JOB (TimelimitRaw ); CREATE INDEX ON :JOB (TotalCPU ); CREATE INDEX ON :JOB (UID ); CREATE INDEX ON :JOB (JOB ); CREATE INDEX ON :JOB (JOBCPU ); CREATE INDEX ON :JOB (WCKeyID ); CREATE INDEX ON :JOB (WorkDir ); CREATE INDEX ON :JOB (ReservedRAW ); CREATE INDEX ON :JOB (TotalCPURAW ); CREATE INDEX ON :JOB (SystemCPURAW ); CREATE INDEX ON :JOB (JOBCPURAW ); CREATE INDEX ON :JOB (billing ); CREATE INDEX ON :JOB (failed ); CREATE INDEX ON :JOB (SubmitRAW ); CREATE INDEX ON :JOB (StartRAW ); CREATE INDEX ON :JOB (EligibleRAW ); CREATE INDEX ON :JOB (EndRAW ); CREATE INDEX ON :JOB (AllocMem ); CREATE INDEX ON :JOB (Dep ); CREATE INDEX ON :JOB (University ); CREATE INDEX ON :JOB (Role ); CREATE INDEX ON :JOB (AllocMemTRES ); CREATE INDEX ON :JOB (ReqMemTRES )"
            )

    def create_sge_index(self):
        with self._driver.session() as session:
            session.run(
                "CREATE INDEX ON :JOB (qname) ;CREATE INDEX ON :JOB (hostname) ;CREATE INDEX ON :JOB (group) ;CREATE INDEX ON :JOB (owner) ;CREATE INDEX ON :JOB (job_name) ;CREATE INDEX ON :JOB (job_number) ;CREATE INDEX ON :JOB (submission_time) ;CREATE INDEX ON :JOB (start_time) ;CREATE INDEX ON :JOB (end_time) ;CREATE INDEX ON :JOB (failed) ;CREATE INDEX ON :JOB (exit_status) ;CREATE INDEX ON :JOB (ru_wallclock) ;CREATE INDEX ON :JOB (ru_utime) ;CREATE INDEX ON :JOB (ru_stime) ;CREATE INDEX ON :JOB (ru_maxrss) ;CREATE INDEX ON :JOB (ru_ixrss) ;CREATE INDEX ON :JOB (ru_ismrss) ;CREATE INDEX ON :JOB (ru_idrss) ;CREATE INDEX ON :JOB (ru_isrss) ;CREATE INDEX ON :JOB (ru_minflt) ;CREATE INDEX ON :JOB (ru_majflt) ;CREATE INDEX ON :JOB (ru_nswap) ;CREATE INDEX ON :JOB (ru_inblock) ;CREATE INDEX ON :JOB (ru_oublock) ;CREATE INDEX ON :JOB (ru_msgsnd) ;CREATE INDEX ON :JOB (ru_msgrcv) ;CREATE INDEX ON :JOB (ru_nsignals) ;CREATE INDEX ON :JOB (ru_nvcsw) ;CREATE INDEX ON :JOB (ru_nivcsw) ;CREATE INDEX ON :JOB (project) ;CREATE INDEX ON :JOB (granted_pe) ;CREATE INDEX ON :JOB (slots) ;CREATE INDEX ON :JOB (task_number) ;CREATE INDEX ON :JOB (cpu) ;CREATE INDEX ON :JOB (mem) ;CREATE INDEX ON :JOB (io) ;CREATE INDEX ON :JOB (category) ;CREATE INDEX ON :JOB (iow) ;CREATE INDEX ON :JOB (pe_taskid) ;CREATE INDEX ON :JOB (maxvmem) ;CREATE INDEX ON :JOB (dep) ;CREATE INDEX ON :JOB (university) ;CREATE INDEX ON :JOB (reqTime) ;CREATE INDEX ON :JOB (reqMem) ;CREATE INDEX ON :JOB (people) ;CREATE INDEX ON :JOB (cumaCPU) ;CREATE INDEX ON :JOB (cumaReqmem) ;CREATE INDEX ON :JOB (minCPU) ;CREATE INDEX ON :JOB (minReqMem) ;CREATE INDEX ON :JOB (maxCPU) ;CREATE INDEX ON :JOB (maxReqMem) ;CREATE INDEX ON :JOB (stdCPU) ;CREATE INDEX ON :JOB (stdReqMem) ;CREATE INDEX ON :JOB (cntJOB) ;CREATE INDEX ON :JOB (killrate) ;CREATE INDEX ON :JOB (q5_experience) ;CREATE INDEX ON :JOB (q6_proficiency) ;CREATE INDEX ON :JOB (q7_training)")

    def load_test_data(self):
        with self._driver.session() as session:
            session.run("USING PERIODIC COMMIT LOAD CSV WITH HEADERS FROM "
                        "'http://people.cs.ksu.edu/~happystep/HPC/test_hpc.csv' AS csvLine "
                        "FIELDTERMINATOR ',' CREATE (j:JOB {"
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
