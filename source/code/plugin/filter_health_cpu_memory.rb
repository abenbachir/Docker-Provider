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
        
        @@previousCpuHealthState = {"State": "", "Time": ""}
        @@previousPreviousCpuHealthState = {"State": "", "Time": ""}
        #@@currentCpuHealthState = ''
        #@@lastEmittedCpuHealthState = ''
        @@previousMemoryRssHealthState = {"State": "", "Time": ""}
        @@previousPreviousMemoryRssHealthState = {"State": "", "Time": ""}
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
                if (healthState == @@previousCpuHealthState['State']) && (healthState == @@previousPreviousCpuHealthState['State'])
                    healthRecord['NodeCpuHealthState'] = healthState
                    healthRecord['NodeCpuUtilizationPercentage'] = metricValue
                    #healthRecord['TimeStateDetected'] = @@previousPreviousCpuHealthState['Time']
                    healthRecord['CollectionTime'] = @@previousPreviousCpuHealthState['Time']
                    updateHealthState = true
                end
                @@previousPreviousCpuHealthState['State'] = @@previousCpuHealthState['State']
                @@previousPreviousCpuHealthState['Time'] = @@previousCpuHealthState['Time']
                @@previousCpuHealthState['State'] = healthState
                @@previousCpuHealthState['Time'] = metricTime
                @@previousCpuHealthState = healthState
            elsif metricName == "memoryRssBytesPercentage"
                @log.debug "metricName: #{metricName}"
                @log.debug "metricValue: #{metricValue}"
                if (healthState == @@previousMemoryRssHealthState['State']) && (healthState == @@previousPreviousMemoryRssHealthState['State'])
                    healthRecord['NodeMemoryRssHealthState'] = healthState
                    healthRecord['NodeMemoryRssPercentage'] = metricValue
                    healthRecord['CollectionTime'] = @@previousMemoryRssHealthState['Time']
                    #healthRecord['TimeStateDetected'] = @@previousMemoryRssHealthState['Time']
                    updateHealthState = true
                end
                @@previousPreviousMemoryRssHealthState['State'] = @@previousMemoryRssHealthState['State']
                @@previousPreviousMemoryRssHealthState['Time'] = @@previousMemoryRssHealthState['Time']
                @@previousMemoryRssHealthState['State'] = healthState
                @@previousMemoryRssHealthState['Time'] = metricTime
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
