# Copyright (c) Microsoft Corporation.  All rights reserved.

# frozen_string_literal: true

module Fluent
    require 'logger'
    require 'json'
    #require_relative 'oms_common'

	class CPUMemoryHealthFilter < Filter
		Fluent::Plugin.register_filter('filter_health_cpu_memory', self)
		
		config_param :enable_log, :integer, :default => 0
        config_param :log_path, :string, :default => '/var/opt/microsoft/omsagent/log/filter_health_cpu_memory.log'
        #config_param :custom_metrics_azure_regions, :string
        #config_param :metrics_to_collect, :string, :default => 'cpuUsageNanoCores,memoryWorkingSetBytes,memoryRssBytes'
        
        @@previousCpuHealthDetails = {"State": "", "Time": "", "Percentage": ""}
        @@previousPreviousCpuHealthDetails = {"State": "", "Time": "", "Percentage": ""}
        @@currentHealthMetrics = {}
        @@nodeCpuHealthDataTimeTracker  = DateTime.now.to_time.to_i
        @@nodeMemoryRssDataTimeTracker  = DateTime.now.to_time.to_i

        #@@lastEmittedCpuHealthState = ''
        @@previousMemoryRssHealthDetails = {"State": "", "Time": "", "Percentage": ""}
        @@previousPreviousMemoryRssHealthDetails = {"State": "", "Time": "", "Percentage": ""}
        @@currentHealthMetrics = {}
        @@clusterName = KubernetesApiClient.getClusterName
        @@clusterId = KubernetesApiClient.getClusterId
        @@clusterRegion = KubernetesApiClient.getClusterRegion
        #@@currentMemoryRssHealthState = ''

		def initialize
			super
		end

		def configure(conf)
			super
			@log = nil
			
			if @enable_log
				@log = Logger.new(@log_path, 'weekly')
				@log.debug {'Starting filter_health_cpu_memory plugin'}
			end
		end

        def start
            super
            @@clusterName = KubernetesApiClient.getClusterName
            @@clusterId = KubernetesApiClient.getClusterId
            @@clusterRegion = KubernetesApiClient.getClusterRegion
        end

		def shutdown
			super
		end

        def processCpuMetrics(cpuMetricValue, cpuMetricPercentValue, healthRecords)
             # Get node CPU usage health
            cpuHealthRecord = {}
            currentCpuHealthDetails = {}
            cpuHealthRecord['ClusterName'] = @@clusterName
            cpuHealthRecord['ClusterId'] = @@clusterId
            cpuHealthRecord['ClusterRegion'] = @@clusterRegion
             cpuHealthState = ''
             if cpuMetricValue.to_f < 80.0
                #nodeCpuHealthState = 'Pass'
                cpuHealthState = "Pass"
             elsif cpuMetricValue.to_f > 90.0
                cpuHealthState = "Fail"
             else
                cpuHealthState = "Warning"
             end
             currentCpuHealthDetails['State'] = cpuHealthState
             currentCpuHealthDetails['Time'] = @@currentHealthMetrics['metricTime']
             currentCpuHealthDetails['CPUUsagePercentage'] = cpuMetricPercentValue
             currentCpuHealthDetails['CPUUsageMillicores'] = cpuMetricValue

             if (cpuHealthState == @@previousCpuHealthDetails['State']) && (cpuHealthState == @@previousPreviousCpuHealthDetails['State'])
                cpuHealthRecord['NodeCpuHealthState'] = cpuHealthState
                cpuHealthRecord['NodeCpuUsagePercentage'] = cpuMetricPercentValue
                cpuHealthRecord['NodeCpuUsageMilliCores'] = cpuMetricValue
                #healthRecord['TimeStateDetected'] = @@previousPreviousCpuHealthDetails['Time']
                cpuHealthRecord['CollectionTime'] = @@previousPreviousCpuHealthDetails['Time']
                cpuHealthRecord['PrevNodeCpuUsageDetails'] = { "Percent": @@previousCpuHealthDetails["CPUUsagePercentage"], "TimeStamp": @@previousCpuHealthDetails["Time"], "Millicores": @@previousCpuHealthDetails['CPUUsageMillicores']}
                cpuHealthRecord['PrevPrevNodeCpuUsageDetails'] = { "Percent": @@previousPreviousCpuHealthDetails["CPUUsagePercentage"], "TimeStamp": @@previousPreviousCpuHealthDetails["Time"], "Millicores": @@previousPreviousCpuHealthDetails['CPUUsageMillicores']}
                updateCpuHealthState = true
            end
            @@previousPreviousCpuHealthDetails = @@previousCpuHealthDetails.clone
            @@previousCpuHealthDetails = currentCpuHealthDetails.clone
            if updateCpuHealthState
                healthRecords.push(cpuHealthRecord)
                @@nodeCpuHealthDataTimeTracker = DateTime.now.to_time.to_i
            end
        end

        def processMemoryRssHealthMetrics(memoryRssMetricValue, memoryRssMetricPercentValue, healthRecords)
             # Get node memory RSS health
            memRssHealthRecord = {}
            currentMemoryRssHealthDetails = {}
            memRssHealthRecord['ClusterName'] = @@clusterName
            memRssHealthRecord['ClusterId'] = @@clusterId
            memRssHealthRecord['ClusterRegion'] = @@clusterRegion
            memoryRssHealthState = ''
             if memoryRssMetricValue.to_f < 80.0
                #nodeCpuHealthState = 'Pass'
                memoryRssHealthState = "Pass"
             elsif memoryRssMetricValue.to_f > 90.0
                memoryRssHealthState = "Fail"
             else
                memoryRssHealthState = "Warning"
             end
             currentMemoryRssHealthDetails['State'] = memoryRssHealthState
             currentMemoryRssHealthDetails['Time'] = @@currentHealthMetrics['metricTime']
             currentMemoryRssHealthDetails['memoryRssPercentage'] = memoryRssMetricPercentValue
             currentMemoryRssHealthDetails['memoryRssBytes'] = memoryRssMetricValue

             if (memoryRssHealthState == @@previousMemoryRssHealthDetails['State']) && (memoryRssHealthState == @@previousPreviousMemoryRssHealthDetails['State'])
                memRssHealthRecord['NodeMemoryRssHealthState'] = memoryRssHealthState
                memRssHealthRecord['NodeMemoryRssPercentage'] = memoryRssMetricPercentValue
                memRssHealthRecord['NodeMemoryRssBytes'] = memoryRssMetricValue
                #healthRecord['TimeStateDetected'] = @@previousPreviousCpuHealthDetails['Time']
                memRssHealthRecord['CollectionTime'] = @@previousPreviousMemoryRssHealthDetails['Time']
                memRssHealthRecord['PrevNodeMemoryRssDetails'] = { "Percent": @@previousMemoryRssHealthDetails["memoryRssPercentage"], "TimeStamp": @@previousMemoryRssHealthDetails["Time"], "Bytes": @@previousMemoryRssHealthDetails['memoryRssBytes']}
                memRssHealthRecord['PrevPrevNodeMemoryRssDetails'] = { "Percent": @@previousPreviousMemoryRssHealthDetails["memoryRssPercentage"], "TimeStamp": @@previousPreviousMemoryRssHealthDetails["Time"], "Bytes": @@previousPreviousMemoryRssHealthDetails['memoryRssBytes']}
                updateMemoryRssHealthState = true
            end
            @@previousPreviousMemoryRssHealthDetails = @@previousMemoryRssHealthDetails.clone
            @@previousMemoryRssHealthDetails = currentMemoryRssHealthDetails.clone
            if updateMemoryRssHealthState
                healthRecords.push(memRssHealthRecord)
                @@nodeMemoryRssDataTimeTracker = DateTime.now.to_time.to_i
            end
        end

        def processHealthMetrics()
            healthRecords = []
            cpuMetricPercentValue = @@currentHealthMetrics['cpuUsageNanoCoresPercentage']
            cpuMetricValue = @@currentHealthMetrics['cpuUsageNanoCores']
            memoryRssMetricPercentValue = @@currentHealthMetrics['memoryRssBytesPercentage']
            memoryRssMetricValue = @@currentHealthMetrics['memoryRssBytes']
            processCpuMetrics(cpuMetricValue, cpuMetricPercentValue, healthRecords)
            processMemoryRssHealthMetrics(memoryRssMetricValue, memoryRssMetricPercentValue, healthRecords)
            return healthRecords
        end

        def filter(tag, time, record)
            # Reading all the records to populate a hash for CPU and memory utilization percentages and values
            @@currentHealthMetrics[record['data']['baseData']['metric']] = record['data']['baseData']['series'][0]['min']
            if !(@@currentHealthMetrics.has_key?("metricTime"))
                @@currentHealthMetrics['metricTime'] = record['time']
            end
            if !(@@currentHealthMetrics.has_key?("computer"))
                @@currentHealthMetrics['computer'] = record['data']['baseData']['series'][0]['dimValues'][0]
            end
            return nil
        end

        def filter_stream(tag, es)
            health_es = MultiEventStream.new
            begin
                es.each { |time, record|
                #begin
                    #filteredRecord = filter(tag, time, record)
                    #currentHealthMetrics['TimeStamp'] = 
                    filter(tag, time, record)
                    #@@currentHealthMetrics.merge!(filteredRecord)
                    #currentHealthMetrics[filteredRecord.keys.first.to_s] = filtered_record[filteredRecord.keys.first.to_s]
                    #TODO: Optimize this to read these values only from first record
                    #processHealthMerics(currentHealthMetrics)
                    #health_es.add(time, filtered_record) if filtered_record
                    #router.emit_stream('oms.rashmi', health_es) if health_es
                #end  
                }
                healthRecords = processHealthMetrics
                healthRecords.each {|healthRecord| 
                    health_es.add(time, healthRecord) if healthRecord
                    router.emit_stream('oms.rashmi', health_es) if health_es
                } if healthRecords
            rescue => e
                router.emit_error_event(tag, time, record, e)
            end
            # Return the event stream as is for mdm perf metrics
            es
        end


	end
end
