import glob
import json
import re
from collections import OrderedDict
from collections import deque

for log in glob.glob("./logs/app-*"):
    appname = ""
    # Build stage dictionary
    stageDict = OrderedDict()
    with open(log) as logfile:
        print(log)
        for line in logfile:
            data = json.loads(line)
            try:
                if data["Event"] == "SparkListenerApplicationStart":
                    appname = data["App Name"]
                if data["Event"] == "SparkListenerStageSubmitted":
                    # print(data)
                    stage = data["Stage Info"]
                    if stage["Stage ID"] not in stageDict.keys():
                        stageDict[stage["Stage ID"]] = {}
                        #if stage["Stage ID"] == 0:
                           # stageDict[0]["totalduration"] = 0
                        stageDict[stage["Stage ID"]]["genstage"] = True if len(stageDict) == 1 else False
                        stageDict[stage["Stage ID"]]["parentsIds"] = stage["Parent IDs"]
                        stageDict[stage["Stage ID"]]["nominalrate"] = 0.0
                        stageDict[stage["Stage ID"]]["weight"] = 0
                        stageDict[stage["Stage ID"]]["RDDIds"] = [x["RDD ID"] for x in stage["RDD Info"]]
                        stageDict[stage["Stage ID"]]["skipped"] = False
                        stageDict[stage["Stage ID"]]["cachedRDDs"] = []
                        for x in stage["RDD Info"]:
                            storage_level = x["Storage Level"]
                            if storage_level["Use Disk"] or storage_level["Use Memory"] or storage_level["Deserialized"]:
                                stageDict[stage["Stage ID"]]["cachedRDDs"].append(x["RDD ID"])

                # if data["Event"] == "SparkListenerStageCompleted":
                #     stageDict[stage["Stage ID"]]["duration"] = stage["Completion Time"] - stage["Submission Time"]
                #     stageDict[0]["totalduration"] += stage["Completion Time"] - stage["Submission Time"]
            except KeyError:
                None

    skipped = []
    with open(log) as logfile:
        for line in logfile:
            data = json.loads(line)
            try:
                if data["Event"] == "SparkListenerJobStart":
                    for stage in data["Stage Infos"]:
                        # print(stage)
                        if stage["Stage ID"] not in stageDict.keys():
                            stageDict[stage["Stage ID"]] = {}
                            #if stage["Stage ID"] == 0:
                               # stageDict[0]["totalduration"] = 0
                            stageDict[stage["Stage ID"]]["genstage"] = True if len(stageDict) == 1 else False
                            stageDict[stage["Stage ID"]]["parentsIds"] = stage["Parent IDs"]
                            stageDict[stage["Stage ID"]]["nominalrate"] = 0.0
                            stageDict[stage["Stage ID"]]["weight"] = 0
                            stageDict[stage["Stage ID"]]["RDDIds"] = [x["RDD ID"] for x in stage["RDD Info"]]
                            stageDict[stage["Stage ID"]]["skipped"] = True
                            stageDict[stage["Stage ID"]]["cachedRDDs"] = []
                            for x in stage["RDD Info"]:
                                storage_level = x["Storage Level"]
                                if storage_level["Use Disk"] or storage_level["Use Memory"] or storage_level[
                                    "Deserialized"]:
                                    stageDict[stage["Stage ID"]]["cachedRDDs"].append(x["RDD ID"])
                            skipped.append(stage["Stage ID"])
                # if data["Event"] == "SparkListenerStageCompleted":
                #     stageDict[stage["Stage ID"]]["duration"] = stage["Completion Time"] - stage["Submission Time"]
                #     stageDict[0]["totalduration"] += stage["Completion Time"] - stage["Submission Time"]
            except KeyError:
                None

    # Replace skipped stage id in parents ids based on RDD IDs
    for skippedId in skipped:
        for stageId1 in stageDict.keys():
            if stageId1 != skippedId and stageDict[skippedId]["RDDIds"] == stageDict[stageId1]["RDDIds"]:
                for stageId2 in stageDict.keys():
                    if skippedId in stageDict[stageId2]["parentsIds"]:
                        stageDict[stageId2]["parentsIds"].remove(skippedId)
                        stageDict[stageId2]["parentsIds"].append(stageId1)

    for stage in stageDict.keys():
        if len(stageDict[stage]["parentsIds"]) == 0:
            try:
                cached = list(stageDict[stage]["cachedRDDs"])
            except KeyError:
                None
            for i in range(0, stage):
                try:
                    for rdd in cached:
                        if rdd in stageDict[i]["cachedRDDs"]:
                            stageDict[stage]["parentsIds"].append(i)
                            cached.remove(rdd)
                except KeyError:
                    None

    print(stageDict)

    # REPEATER = re.compile(r"(.+?)\1+$")
    # def repeated(s):
    #     match = REPEATER.match(s)
    #     return match.group(1) if match else None
    #
    # # Find iterations
    # lenparent = []
    # for key in stageDict.keys():
    #     lenparent.append(str(len(stageDict[key]['Parent IDs'])))
    # i = 0
    # stage_repeated = None
    # while stage_repeated == None and i < len(lenparent):
    #     stage_repeated = repeated("".join(lenparent[i:]))
    #     i += 1
    # print(i, stage_repeated)

    def setWeight(key):
        for parentid in stageDict[key]['parentsIds']:
            w1 = stageDict[key]["weight"] + 1
            w2 = stageDict[parentid]["weight"]
            stageDict[parentid]["weight"] = max(w1, w2)
            setWeight(parentid)

    # Set weights
    for key in reversed(stageDict.keys()):
        setWeight(key)

    # for key in stageDict.keys():
    #     try:
    #         print("SID " + str(key) + " " + str(stageDict[key]["duration"] / stageDict[0]["totalduration"]))
    #         oldWeight = stageDict[key]["weight"]
    #         stageDict[key]["weight"] = stageDict[key]["weight"] / (stageDict[key]["duration"] / stageDict[0]["totalduration"])
    #         print(oldWeight, stageDict[key]["weight"])
    #     except KeyError:
    #         None

    # Create json output
    with open("./json/" + re.sub("[^a-zA-Z0-9.-]", "_", appname) + ".json", "w") as jsonoutput:
        json.dump(stageDict, jsonoutput, indent=4, sort_keys=True)