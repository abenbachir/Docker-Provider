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
        end

		def shutdown
			super
		end

        def filter(tag, time, record)
            healthRecord = {}
            #hostName = (OMS::Common.get_hostname)
            #currentTime = Time.now
            #batchTime = currentTime.utc.iso8601
            #healthRecord['CollectionTime'] = batchTime #This is the time that is mapped to become TimeGenerated
            healthRecord['ClusterName'] = @@clusterName
            healthRecord['ClusterId'] = @@clusterId
            healthRecord['ClusterRegion'] = @@clusterRegion
            healthRecord['Computer'] = record['data']['baseData']['series'][0]['dimValues'][0]
            metricTime = record['time']
            metricName = record['data']['baseData']['metric']
            metricValue = record['data']['baseData']['series'][0]['min']
            updateHealthState = false
            if metricValue_f < 80.0
                #nodeCpuHealthState = 'Pass'
                healthState = "Pass"
             elsif metricValue_f > 90.0
                healthState = "Fail"
             else
                healthState = "Warning"
             end
            if metricName == "cpuUsageNanoCoresPercentage"
                @log.debug "metricName: #{metricName}"
                @log.debug "metricValue: #{metricValue}"
                #currentCpuHealthState = ""
                if (healthState == @@previousCpuHealthDetails['State']) && (healthState == @@previousPreviousCpuHealthDetails['State'])
                    healthRecord['NodeCpuHealthState'] = healthState
                    healthRecord['NodeCpuUtilizationPercentage'] = metricValue
                    #healthRecord['TimeStateDetected'] = @@previousPreviousCpuHealthDetails['Time']
                    healthRecord['CollectionTime'] = @@previousPreviousCpuHealthDetails['Time']
                    healthRecord['PrevNodeCpuUtilizationDetails'] = { "Percent": @@previousCpuHealthDetails["Percentage"], "TimeStamp": @@previousCpuHealthDetails["Time"]}
                    healthRecord['PrevPrevNodeCpuUtilizationDetails'] = { "Percent": @@previousPreviousCpuHealthDetails["Percentage"], "TimeStamp": @@previousPreviousCpuHealthDetails["Time"]}
                    updateHealthState = true
                end
                @@previousPreviousCpuHealthDetails['State'] = @@previousCpuHealthDetails['State']
                @@previousPreviousCpuHealthDetails['Time'] = @@previousCpuHealthDetails['Time']
                @@previousCpuHealthDetails['State'] = healthState
                @@previousCpuHealthDetails['Time'] = metricTime
                @@previousCpuHealthDetails = healthState
            elsif metricName == "memoryRssBytesPercentage"
                @log.debug "metricName: #{metricName}"
                @log.debug "metricValue: #{metricValue}"
                if (healthState == @@previousMemoryRssHealthDetails['State']) && (healthState == @@previousPreviousMemoryRssHealthDetails['State'])
                    healthRecord['NodeMemoryRssHealthState'] = healthState
                    healthRecord['NodeMemoryRssPercentage'] = metricValue
                    healthRecord['CollectionTime'] = @@previousMemoryRssHealthDetails['Time']
                    healthRecord['PrevNodeMemoryRssDetails'] = { "Percent": @@previousMemoryRssHealthDetails["Percentage"], "TimeStamp": @@previousMemoryRssHealthDetails["Time"]}
                    healthRecord['PrevPrevNodeMemoryRssDetails'] = { "Percent": @@previousPreviousMemoryRssHealthDetails["Percentage"], "TimeStamp": @@previousPreviousMemoryRssHealthDetails["Time"]}
                    #healthRecord['TimeStateDetected'] = @@previousMemoryRssHealthDetails['Time']
                    updateHealthState = true
                end
                @@previousPreviousMemoryRssHealthDetails['State'] = @@previousMemoryRssHealthDetails['State']
                @@previousPreviousMemoryRssHealthDetails['Time'] = @@previousMemoryRssHealthDetails['Time']
                @@previousMemoryRssHealthDetails['State'] = healthState
                @@previousMemoryRssHealthDetails['Time'] = metricTime
            end
            if updateHealthState
                return healthRecord
            else
                return nil
            end
        end

        def processHealthMetrics()
            healthRecord = {}
            currentCpuHealthDetails = {}
            cpuMetricPercentValue = @@currentHealthMetrics['cpuUsageNanoCoresPercentage']
            cpuMetricValue = @@currentHealthMetrics['cpuUsageNanoCores']
            memoryRssMetricPercentValue = @@currentHealthMetrics['memoryRssBytesPercentage']
            memoryRssMetricValue = @@currentHealthMetrics['memoryRssBytes']
            updateHealthState = false
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
             currentCpuHealthDetails['CPUUtilPercentage'] = cpuMetricPercentValue
             currentCpuHealthDetails['CPUUsageMillicores'] = cpuMetricValue

             if (cpuHealthState == @@previousCpuHealthDetails['State']) && (cpuHealthState == @@previousPreviousCpuHealthDetails['State'])
                healthRecord['NodeCpuHealthState'] = cpuHealthState
                healthRecord['NodeCpuUtilizationPercentage'] = cpuMetricPercentValue
                healthRecord['NodeCpuMilliCores'] = cpuMetricValue
                #healthRecord['TimeStateDetected'] = @@previousPreviousCpuHealthDetails['Time']
                healthRecord['CollectionTime'] = @@previousPreviousCpuHealthDetails['Time']
                healthRecord['PrevNodeCpuUtilizationDetails'] = { "Percent": @@previousCpuHealthDetails["Percentage"], "TimeStamp": @@previousCpuHealthDetails["Time"], "Millicores": @@previousCpuHealthDetails['CPUUsageMillicores']}
                healthRecord['PrevPrevNodeCpuUtilizationDetails'] = { "Percent": @@previousPreviousCpuHealthDetails["Percentage"], "TimeStamp": @@previousPreviousCpuHealthDetails["Time"], "Millicores": @@previousPreviousCpuHealthDetails['CPUUsageMillicores']}
                updateHealthState = true
            end
            @@previousPreviousCpuHealthDetails['State'] = @@previousCpuHealthDetails['State']
            @@previousPreviousCpuHealthDetails['Time'] = @@previousCpuHealthDetails['Time']
            @@previousCpuHealthDetails['State'] = healthState
            @@previousCpuHealthDetails['Time'] = metricTime
            @@previousCpuHealthDetails = healthState


        end

        def filter(tag, time, record)
            # Reading all the records to populate a hash for CPU and memory utilization percentages and values
            #metricRecord = {}
            #metricRecord[record['data']['baseData']['metric']] = record['data']['baseData']['series'][0]['min']
            @@currentHealthMetrics[record['data']['baseData']['metric']] = record['data']['baseData']['series'][0]['min']
            if !(@@currentHealthMetrics.has_key?("metricTime"))
                @@currentHealthMetrics['metricTime'] = record['time']
            end
            if !(@@currentHealthMetrics.has_key?("computer"))
                @@currentHealthMetrics['computer'] = record['data']['baseData']['series'][0]['dimValues'][0]
            end
            #@@currentHealthMetrics[record['data']['baseData']['metric']] = record['data']['baseData']['series'][0]['min']
            #@@currentHealthMetrics
            return nil
        end

        def filter_stream(tag, es)
            health_es = MultiEventStream.new
            #currentHealthMetrics = {}
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
                processHealthMetrics
            rescue => e
                router.emit_error_event(tag, time, record, e)
            end
            es
        end


	end
end
