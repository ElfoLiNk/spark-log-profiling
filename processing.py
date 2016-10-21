import glob
import re
from collections import OrderedDict

import numpy as np

import json

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
                        if stage["Stage ID"] == 0:
                            stageDict[0]["totalduration"] = 0
                        stageDict[stage["Stage ID"]]["name"] = stage['Stage Name']
                        stageDict[stage["Stage ID"]]["genstage"] = False
                        stageDict[stage["Stage ID"]]["parentsIds"] = stage["Parent IDs"]
                        stageDict[stage["Stage ID"]]["nominalrate"] = 0.0
                        stageDict[stage["Stage ID"]]["weight"] = 0
                        stageDict[stage["Stage ID"]]["RDDIds"] = [x["RDD ID"] for x in stage["RDD Info"]]
                        stageDict[stage["Stage ID"]]["skipped"] = False
                        stageDict[stage["Stage ID"]]["cachedRDDs"] = []
                        stageDict[stage["Stage ID"]]["numtask"] = 0
                        for x in stage["RDD Info"]:
                            storage_level = x["Storage Level"]
                            if storage_level["Use Disk"] or storage_level["Use Memory"] or storage_level[
                                "Deserialized"]:
                                stageDict[stage["Stage ID"]]["cachedRDDs"].append(x["RDD ID"])
                if data["Event"] == "SparkListenerStageCompleted":
                    # print(data)
                    stageDict[data["Stage Info"]["Stage ID"]]["numtask"] = data["Stage Info"]['Number of Tasks']
                    for acc in data["Stage Info"]["Accumulables"]:
                        if acc["Name"] == "internal.metrics.executorRunTime":
                            stageDict[data["Stage Info"]["Stage ID"]]["duration"] = int(acc["Value"])
                            stageDict[0]["totalduration"] += int(acc["Value"])
                        if acc["Name"] == "internal.metrics.input.recordsRead":
                            stageDict[data["Stage Info"]["Stage ID"]]["recordsread"] = acc["Value"]
                        else:
                            try:
                                stageDict[data["Stage Info"]["Stage ID"]]["recordsread"]
                            except KeyError:
                                stageDict[data["Stage Info"]["Stage ID"]]["recordsread"] = 0.0
                        if acc["Name"] == "internal.metrics.shuffle.read.recordsRead":
                            stageDict[data["Stage Info"]["Stage ID"]]["shufflerecordsread"] = acc["Value"]
                        else:
                            try:
                                stageDict[data["Stage Info"]["Stage ID"]]["shufflerecordsread"]
                            except KeyError:
                                stageDict[data["Stage Info"]["Stage ID"]]["shufflerecordsread"] = 0.0
                        if acc["Name"] == "internal.metrics.output.recordsWrite":
                            stageDict[data["Stage Info"]["Stage ID"]]["recordswrite"] = acc["Value"]
                        else:
                            try:
                                stageDict[data["Stage Info"]["Stage ID"]]["recordswrite"]
                            except KeyError:
                                stageDict[data["Stage Info"]["Stage ID"]]["recordswrite"] = 0.0
                        if acc["Name"] == "internal.metrics.shuffle.write.recordsWritten":
                            stageDict[data["Stage Info"]["Stage ID"]]["shufflerecordswrite"] = acc["Value"]
                        else:
                            try:
                                stageDict[data["Stage Info"]["Stage ID"]]["shufflerecordswrite"]
                            except KeyError:
                                stageDict[data["Stage Info"]["Stage ID"]]["shufflerecordswrite"] = 0.0
            except KeyError:
                print(data)

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
                            stageDict[stage["Stage ID"]]["genstage"] = True if len(stageDict) == 1 else False
                            stageDict[stage["Stage ID"]]["parentsIds"] = stage["Parent IDs"]
                            stageDict[stage["Stage ID"]]["nominalrate"] = 0.0
                            stageDict[stage["Stage ID"]]["weight"] = 0
                            stageDict[stage["Stage ID"]]["duration"] = 0
                            stageDict[stage["Stage ID"]]["RDDIds"] = [x["RDD ID"] for x in stage["RDD Info"]]
                            stageDict[stage["Stage ID"]]["skipped"] = True
                            stageDict[stage["Stage ID"]]["cachedRDDs"] = []
                            for x in stage["RDD Info"]:
                                storage_level = x["Storage Level"]
                                if storage_level["Use Disk"] or storage_level["Use Memory"] or storage_level[
                                    "Deserialized"]:
                                    stageDict[stage["Stage ID"]]["cachedRDDs"].append(x["RDD ID"])
                            skipped.append(stage["Stage ID"])
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

    # def setWeight(key):
    #     for parentid in stageDict[key]['parentsIds']:
    #         w1 = stageDict[key]["weight"] + 1
    #         w2 = stageDict[parentid]["weight"]
    #         stageDict[parentid]["weight"] = max(w1, w2)
    #         setWeight(parentid)
    #
    # # Set weights
    # for key in reversed(stageDict.keys()):
    #     setWeight(key)

    stageToDo = len(list(stageDict.keys())) - len(skipped)
    for stageId in sorted(stageDict.keys()):
        parentOutput = 0
        parentInput = 0
        if stageId not in skipped:
            stageDict[stageId]["weight"] = stageToDo
            stageToDo -= 1
            for parentID in stageDict[stageId]["parentsIds"]:
                parentOutput += stageDict[parentID]["recordswrite"]
                parentOutput += stageDict[parentID]["shufflerecordswrite"]
                parentInput += stageDict[parentID]["recordsread"]
                parentInput += stageDict[parentID]["shufflerecordsread"]
            if parentOutput != 0:
                stageDict[stageId]["nominalrate"] = parentOutput / (stageDict[stageId]["duration"] / 1000.0)
            elif parentInput != 0:
                stageDict[stageId]["nominalrate"] = parentInput / (stageDict[stageId]["duration"] / 1000.0)
            else:
                stageInput = stageDict[stageId]["recordsread"] + stageDict[stageId]["shufflerecordsread"]
                if stageInput != 0 and stageInput != stageDict[stageId]["numtask"]:
                    stageDict[stageId]["nominalrate"] = stageInput / (stageDict[stageId]["duration"] / 1000.0)
                else:
                    stageOutput = stageDict[stageId]["recordswrite"] + stageDict[stageId]["shufflerecordswrite"]
                    stageDict[stageId]["nominalrate"] = stageInput / (stageDict[stageId]["duration"] / 1000.0)
            if stageDict[stageId]["nominalrate"] == 0.0:
                stageDict[stageId]["genstage"] = True

    totalduration = stageDict[0]["totalduration"]
    for key in stageDict.keys():
        if key not in skipped:
            oldWeight = stageDict[key]["weight"]
            stageDict[key]["weight"] = np.mean([oldWeight, totalduration / stageDict[key]["duration"]])
            totalduration -= stageDict[key]["duration"]

    # Create json output
    with open("./json/" + re.sub("[^a-zA-Z0-9.-]", "_", appname) + ".json", "w") as jsonoutput:
        json.dump(stageDict, jsonoutput, indent=4, sort_keys=True)
