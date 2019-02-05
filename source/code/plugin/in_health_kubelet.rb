#!/usr/local/bin/ruby
# frozen_string_literal: true

module Fluent
    
      class Kubelet_Health_Input < Input
        Plugin.register_input('kubelethealth', self)
    
        def initialize
          super
          require 'yaml'
          require 'json'
    
          require_relative 'KubernetesApiClient'
          require_relative 'oms_common'
          require_relative 'omslog'
          require_relative 'ApplicationInsightsUtility'

        end
    
        config_param :run_interval, :time, :default => '1m'
        config_param :tag, :string, :default => "oms.containerinsights.KubeletHealth"
    
        def configure (conf)
          super
        end
    
        def start
          if @run_interval
            @finished = false
            @condition = ConditionVariable.new
            @mutex = Mutex.new
            @thread = Thread.new(&method(:run_periodic))
            @@previousNodeStatus = {}
            # Tracks the last time node health data sent for each node
            @@nodeHealthDataTimeTracker = {}
          end
        end
    
        def shutdown
          if @run_interval
            @mutex.synchronize {
              @finished = true
              @condition.signal
            }
            @thread.join
          end
        end
    
        def populateCommonfields(record)
          record['ClusterName'] = KubernetesApiClient.getClusterName
          record['ClusterId'] = KubernetesApiClient.getClusterId
          record['ClusterRegion'] = KubernetesApiClient.getClusterRegion
        end

        def enumerate
            currentTime = Time.now
            emitTime = currentTime.to_f
            batchTime = currentTime.utc.iso8601
            $log.info("in_health_health::enumerate : Getting nodes from Kube API @ #{Time.now.utc.iso8601}")
            nodeInventory = JSON.parse(KubernetesApiClient.getKubeResourceInfo('nodes').body)
            $log.info("in_health_health::enumerate : Done getting nodes from Kube API @ #{Time.now.utc.iso8601}")
              begin
                if(!nodeInventory.empty?)
                  eventStream = MultiEventStream.new
                  #containerNodeInventoryEventStream = MultiEventStream.new 
                    #get node inventory 
                    nodeInventory['items'].each do |item|
                        record = {}
                        record['CollectionTime'] = batchTime #This is the time that is mapped to become TimeGenerated
                        computerName = item['metadata']['name'] 
                        record['Computer'] = computerName
                        #record['ClusterName'] = KubernetesApiClient.getClusterName
                        #record['ClusterId'] = KubernetesApiClient.getClusterId
                        #record['ClusterRegion'] = KubernetesApiClient.getClusterRegion
                        #record['Status'] = ""
                        # Tracking state change in order to send node health data only in case of state change or timeout
                        #isStateChange = false
                        flushRecord = false

                        currentTime = DateTime.now.to_time.to_i
                        timeDifferenceInMinutes = 0
                        if !@@nodeHealthDataTimeTracker[computerName].nil?
                          timeDifference =  (currentTime - @@nodeHealthDataTimeTracker[computerName]).abs
                          timeDifferenceInMinutes = timeDifference/60
                        end
                        if item['status'].key?("conditions") && !item['status']['conditions'].empty?
                          allNodeConditions="" 
                          item['status']['conditions'].each do |condition|
                            conditionType = condition['type']
                            conditionStatus = condition['status']
                            conditionReason = condition['reason']
                            if @@previousNodeStatus[computerName+conditionType].nil? || 
                              !(conditionStatus.casecmp(@@previousNodeStatus[computerName+conditionType]) == 0) ||
                              timeDifferenceInMinutes >= 3
                              # Comparing current status with previous status and setting state change as true
                              #isStateChange = true
                              flushRecord = true
                              @@previousNodeStatus[computerName+conditionType] = conditionStatus
                              #if conditionType == "Ready"
                                #record['KubeletReadyStatus'] = conditionStatus
                                #record['KubeletStatusMessage'] = condition['message']
                                #record['KubeletStatusReason'] = conditionReason
                              #elsif conditionStatus == "True" || conditionStatus == "Unknown"
                                if !allNodeConditions.empty?
                                  allNodeConditions = allNodeConditions + "," + conditionType + ":"  + conditionReason
                                else
                                  allNodeConditions = conditionType + ":" + conditionReason
                                end
                              #end
                              if !allNodeConditions.empty?
                                record['NodeStatusCondition'] = allNodeConditions
                              end
                            end
                          end
                      end
                      
                      #currentTime = DateTime.now.to_time.to_i
                      #timeDifferenceInMinutes = 0
                      #if !@@nodeHealthDataTimeTracker[computerName].nil?
                        #timeDifference =  (currentTime - @@nodeHealthDataTimeTracker[computerName]).abs
                        #timeDifferenceInMinutes = timeDifference/60
                      #end
                      #if (isStateChange) || (timeDifferenceInMinutes >= 3)
                      if flushRecord
                        #Sending node health data the very first time without checking for state change and timeout
                        record['Computer'] = computerName
                        populateCommonfields(record)
                        $log.warn("recordData: #{record}")
                        eventStream.add(emitTime, record) if record
                        @@nodeHealthDataTimeTracker[computerName] = currentTime
                      end
                    end
                    router.emit_stream(@tag, eventStream) if eventStream
                end  
              rescue  => errorStr
                #$log.warn line.dump, error: errorStr.to_s
                #$log.debug_backtrace(e.backtrace)
                $log.warn("error : #{errorStr.to_s}")
                #ApplicationInsightsUtility.sendExceptionTelemetry(errorStr)
              end   
        end
    
        def run_periodic
          @mutex.lock
          done = @finished
          until done
            @condition.wait(@mutex, @run_interval)
            done = @finished
            @mutex.unlock
            if !done
              begin
                $log.info("in_health_kubelet::run_periodic @ #{Time.now.utc.iso8601}")
                enumerate
              rescue => errorStr
                $log.warn "in_health_kubelet::run_periodic: enumerate Failed for kubelet health: #{errorStr}"
                ApplicationInsightsUtility.sendExceptionTelemetry(errorStr)
              end
            end
            @mutex.lock
          end
          @mutex.unlock
        end
    
      end # Health_Kubelet_Input
    
    end # module
    
    