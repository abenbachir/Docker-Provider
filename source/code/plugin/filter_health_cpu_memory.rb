# Copyright (c) Microsoft Corporation.  All rights reserved.

# frozen_string_literal: true

module Fluent
    require 'logger'
    require 'json'
    require_relative 'omslog'

	class CPUMemoryHealthFilter < Filter
		Fluent::Plugin.register_filter('filter_health_cpu_memory', self)
		
		config_param :enable_log, :integer, :default => 0
        config_param :log_path, :string, :default => '/var/opt/microsoft/omsagent/log/filter_health_cpu_memory.log'
        
        @@previousCpuHealthDetails = {}
        @@previousPreviousCpuHealthDetails = {}
        @@currentHealthMetrics = {}
        @@nodeCpuHealthDataTimeTracker  = DateTime.now.to_time.to_i
        @@nodeMemoryRssDataTimeTracker  = DateTime.now.to_time.to_i

        @@previousMemoryRssHealthDetails = {}
        @@previousPreviousMemoryRssHealthDetails = {}
        @@currentHealthMetrics = {}
        @@clusterName = KubernetesApiClient.getClusterName
        @@clusterId = KubernetesApiClient.getClusterId
        @@clusterRegion = KubernetesApiClient.getClusterRegion

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
            @@cpu_limit = 0.0
            @@memory_limit = 0.0 
            begin 
                nodeInventory = JSON.parse(KubernetesApiClient.getKubeResourceInfo('nodes').body)
            rescue Exception => e
                @log.info "Error when getting nodeInventory from kube API. Exception: #{e.class} Message: #{e.message} "
                ApplicationInsightsUtility.sendExceptionTelemetry(e.backtrace)
            end
            if !nodeInventory.nil? 
                cpu_limit_json = KubernetesApiClient.parseNodeLimits(nodeInventory, "capacity", "cpu", "cpuCapacityNanoCores")
                if !cpu_limit_json.nil? 
                    @@cpu_limit = cpu_limit_json[0]['DataItems'][0]['Collections'][0]['Value']
                    @log.info "CPU Limit #{@@cpu_limit}"
                else
                    @log.info "Error getting cpu_limit"
                end
                memory_limit_json = KubernetesApiClient.parseNodeLimits(nodeInventory, "capacity", "memory", "memoryCapacityBytes")
                if !memory_limit_json.nil?
                    @@memory_limit = memory_limit_json[0]['DataItems'][0]['Collections'][0]['Value']
                    @log.info "Memory Limit #{@@memory_limit}"
                else
                    @log.info "Error getting memory_limit"
                end
            end
        end

		def shutdown
			super
		end

        def processCpuMetrics(cpuMetricValue, cpuMetricPercentValue, healthRecords)
            begin
                @log.debug "cpuMetricValue: #{cpuMetricValue}"
                @log.debug "cpuMetricPercentValue: #{cpuMetricPercentValue}"
                #@log.debug "healthRecords: #{healthRecords}"
             # Get node CPU usage health
            updateCpuHealthState = false
            cpuHealthRecord = {}
            currentCpuHealthDetails = {}
            cpuHealthRecord['ClusterName'] = @@clusterName
            cpuHealthRecord['ClusterId'] = @@clusterId
            cpuHealthRecord['ClusterRegion'] = @@clusterRegion
            cpuHealthRecord['Computer'] = @@currentHealthMetrics['computer']
             cpuHealthState = ''
             if cpuMetricPercentValue.to_f < 80.0
                #nodeCpuHealthState = 'Pass'
                cpuHealthState = "Pass"
             elsif cpuMetricPercentValue.to_f > 90.0
                cpuHealthState = "Fail"
             else
                cpuHealthState = "Warning"
             end
             currentCpuHealthDetails['State'] = cpuHealthState
             currentCpuHealthDetails['Time'] = @@currentHealthMetrics['metricTime']
             currentCpuHealthDetails['CPUUsagePercentage'] = cpuMetricPercentValue
             currentCpuHealthDetails['CPUUsageMillicores'] = cpuMetricValue

            currentTime = DateTime.now.to_time.to_i
            timeDifference =  (currentTime - @@nodeCpuHealthDataTimeTracker).abs
            timeDifferenceInMinutes = timeDifference/60

             if ( @@previousCpuHealthDetails['State'].nil? ||
                 ((cpuHealthState == @@previousCpuHealthDetails['State']) && (cpuHealthState == @@previousPreviousCpuHealthDetails['State'])) ||
                 timeDifferenceInMinutes > 50)
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
                @log.debug "cpu health record: #{cpuHealthRecord}"
                healthRecords.push(cpuHealthRecord)
                @@nodeCpuHealthDataTimeTracker = DateTime.now.to_time.to_i
            end
            rescue => errorStr
                @log.debug "In processCpuMetrics: exception: #{errorStr}"
            end
        end

        def processMemoryRssHealthMetrics(memoryRssMetricValue, memoryRssMetricPercentValue, healthRecords)
            begin
                @log.debug "memoryRssMetricValue: #{memoryRssMetricValue}"
                @log.debug "memoryRssMetricPercentValue: #{memoryRssMetricPercentValue}"
                #@log.debug "healthRecords: #{healthRecords}"

             # Get node memory RSS health
            memRssHealthRecord = {}
            currentMemoryRssHealthDetails = {}
            memRssHealthRecord['ClusterName'] = @@clusterName
            memRssHealthRecord['ClusterId'] = @@clusterId
            memRssHealthRecord['ClusterRegion'] = @@clusterRegion
            memRssHealthRecord['Computer'] = @@currentHealthMetrics['computer']

            memoryRssHealthState = ''
             if memoryRssMetricPercentValue.to_f < 80.0
                #nodeCpuHealthState = 'Pass'
                memoryRssHealthState = "Pass"
             elsif memoryRssMetricPercentValue.to_f > 90.0
                memoryRssHealthState = "Fail"
             else
                memoryRssHealthState = "Warning"
             end
             currentMemoryRssHealthDetails['State'] = memoryRssHealthState
             currentMemoryRssHealthDetails['Time'] = @@currentHealthMetrics['metricTime']
             currentMemoryRssHealthDetails['memoryRssPercentage'] = memoryRssMetricPercentValue
             currentMemoryRssHealthDetails['memoryRssBytes'] = memoryRssMetricValue

            currentTime = DateTime.now.to_time.to_i
            timeDifference =  (currentTime - @@nodeMemoryRssDataTimeTracker).abs
            timeDifferenceInMinutes = timeDifference/60

             if (@@previousMemoryRssHealthDetails['State'].nil? ||
                 ((memoryRssHealthState == @@previousMemoryRssHealthDetails['State']) && (memoryRssHealthState == @@previousPreviousMemoryRssHealthDetails['State'])) ||
                 timeDifferenceInMinutes > 50)
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
                @log.debug "memory health record: #{memRssHealthRecord}"
                healthRecords.push(memRssHealthRecord)
                @@nodeMemoryRssDataTimeTracker = currentTime
            end
            rescue => errorStr
                @log.debug "In processMemoryRssMetrics: exception: #{errorStr}"
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
            #timeFromEventStream = nil
            timeFromEventStream = DateTime.now.to_time.to_i
            begin
                es.each { |time, record|
                    filter(tag, time, record)
                    if !timeFromEventStream.nil?
                        timeFromEventStream = time
                    end
                }
                healthRecords = processHealthMetrics
                healthRecords.each {|healthRecord| 
                    health_es.add(timeFromEventStream, healthRecord) if healthRecord
                    router.emit_stream('oms.rashmi', health_es) if health_es
                } if healthRecords
            rescue => e
                #router.emit_error_event(tag, timeFromEventStream, record, e)
                @log.debug "exception: #{e}"
            end
            # Return the event stream as is for mdm perf metrics
            es
        end


	end
end
