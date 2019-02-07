# Copyright (c) Microsoft Corporation.  All rights reserved.

# frozen_string_literal: true

module Fluent
    require 'logger'
    require 'json'
    require_relative 'oms_common'

	class CPUMemoryHealthFilter < Filter
		Fluent::Plugin.register_filter('filter_health_cpu_memory', self)
		
		config_param :enable_log, :integer, :default => 0
        config_param :log_path, :string, :default => '/var/opt/microsoft/omsagent/log/filter_health_cpu_memory.log'
        #config_param :custom_metrics_azure_regions, :string
        #config_param :metrics_to_collect, :string, :default => 'cpuUsageNanoCores,memoryWorkingSetBytes,memoryRssBytes'
        
        @@previousCpuHealthDetails = {"State": "", "Time": "", "Percentage": ""}
        @@previousPreviousCpuHealthDetails = {"State": "", "Time": "", "Percentage": ""}
        #@@currentCpuHealthState = ''
        #@@lastEmittedCpuHealthState = ''
        @@previousMemoryRssHealthDetails = {"State": "", "Time": "", "Percentage": ""}
        @@previousPreviousMemoryRssHealthDetails = {"State": "", "Time": "", "Percentage": ""}
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
            hostName = (OMS::Common.get_hostname)
            currentTime = Time.now
            batchTime = currentTime.utc.iso8601
            #healthRecord['CollectionTime'] = batchTime #This is the time that is mapped to become TimeGenerated
            healthRecord['Computer'] = hostName
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
                    healthRecord['PrevNodeCpuUtilizationPercentage'] = { "Percent": @@previousCpuHealthDetails["Percentage"], "TimeStamp": @@previousCpuHealthDetails["Time"]}
                    healthRecord['PrevPrevNodeCpuUtilizationPercentage'] = { "Percent": @@previousPreviousCpuHealthDetails["Percentage"], "TimeStamp": @@previousPreviousCpuHealthDetails["Time"]}
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

        def filter_stream(tag, es)
            health_es = MultiEventStream.new
            es.each { |time, record|
              begin
                filtered_record = filter(tag, time, record)
                #filtered_records.each {|filtered_record| 
                    health_es.add(time, filtered_record) if filtered_record
                    router.emit_stream('oms.rashmi', health_es) if health_es
                #} if filtered_records
              rescue => e
                router.emit_error_event(tag, time, record, e)
              end
            }
            es
        end


	end
end
