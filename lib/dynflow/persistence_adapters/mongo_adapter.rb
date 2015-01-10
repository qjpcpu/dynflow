require 'mongoid'

class DynflowExecutionPlanStorage
    include Mongoid::Document
    has_many :dynflow_action_storages
    has_many :dynflow_step_storages
    field :uuid, type: String
    field :state, type: String
    field :result,type: String
    field :started_at, type: DateTime
    field :ended_at, type: DateTime
    field :real_time, type: Float
    field :execution_time, type: Float
    field :data, type: Hash, default: ->{{}}

    def load_content
        HashWithIndifferentAccess.new self.fields.keys.inject({}){|memo,k| memo[k]=self.send k;memo}
    end
    def save_content!(value)
        list = [:uuid,:state,:result,:started_at,:ended_at,:real_time,:execution_time]
        n_value = value.inject({}) do |memo,(k,v)|
            case k
            when :id
                memo[:uuid] = v
            when *list
                memo[k] = v
            end
            memo
        end
        n_value[:data]=value
        self.update_attributes! n_value if n_value
        value
    end
end

class DynflowActionStorage
    include Mongoid::Document
    belongs_to :dynflow_execution_plan_storage
    has_many :dynflow_step_storages

    field :action_id, type: Integer
    field :data, type: Hash, default: ->{{}}

    def load_content
        HashWithIndifferentAccess.new({
                                          :data=>self.data,
                                          :action_id=>self.action_id,
                                          :execution_plan_uuid=>self.dynflow_execution_plan_storage.uuid
                                      })
    end
    def save_content!(value)
        n_value = value.inject({}) do |memo,(k,v)|
            case k
            when :id
                memo[:action_id] = v
            end
            memo
        end
        n_value[:data]=value
        self.update_attributes! n_value if n_value
        value
    end
end

class DynflowStepStorage
    include Mongoid::Document
    belongs_to :dynflow_execution_plan_storage
    belongs_to :dynflow_action_storage
    field :data, type: Hash, default: ->{{}}
    field :state,type: String
    field :started_at, type: DateTime
    field :ended_at, type: DateTime
    field :real_time, type: Float
    field :execution_time, type: Float
    field :progress_done, type: Float
    field :progress_weight, type: Float
    field :step_id, type: Integer

    def load_content
        self.fields.keys.inject({}){|memo,k| memo[k]=self.send k;memo}
        HashWithIndifferentAccess.new({
                                          :data=>self.data,
                                          :state=>self.state,
                                          :started_at=>self.started_at,
                                          :ended_at=>self.ended_at,
                                          :real_time=>self.real_time,
                                          :execution_time=>self.execution_time,
                                          :progress_done=>self.progress_done,
                                          :progress_weight=>self.progress_weight,
                                          :step_id=>self.step_id,
                                          :execution_plan_uuid=>self.dynflow_execution_plan_storage.uuid,
                                          :action_id=>self.dynflow_action_storage.action_id
                                      })
    end
    def save_content!(value)
        list = [:state,:started_at,:ended_at,:real_time,:execution_time,:progress_done,:progress_weight]
        n_value = value.inject({}) do |memo,(k,v)|
            case k
            when :id
                memo[:step_id] = v
            when *list
                memo[k]=v
            end
            memo
        end
        n_value[:data]=value
        self.update_attributes! n_value if n_value
        if value[:action_id]
            self.dynflow_action_storage = self.dynflow_execution_plan_storage.dynflow_action_storages.find_or_create_by :action_id=>value[:action_id]
            save!
        end
        value
    end

end

module Dynflow
    module PersistenceAdapters
        class MongoAdapter < Abstract
            def pagination?
                true
            end

            def filtering_by
                META_DATA.fetch :execution_plan
            end

            def ordering_by
                META_DATA.fetch :execution_plan
            end

            META_DATA = { execution_plan: %w(state result started_at ended_at real_time execution_time),
                action:         [],
                step:           %w(state started_at ended_at real_time execution_time action_id progress_done progress_weight) }


            def find_execution_plans(options = {})
                options=HashWithIndifferentAccess.new options
                data_set = DynflowExecutionPlanStorage.all
                # filter
                if options[:filters]
                    options[:filters].each do |k,v|
                        if v.is_a? Array
                            data_set = data_set.in k=>v
                        else
                            data_set = data_set.where k=>v
                        end
                    end
                end
                # order
                if options[:order_by]
                    data_set = data_set.order_by{|e| e.send options[:order_by] }
                    data_set = data_set.reverse if options[:desc]
                end
                # page
                if options[:page]  && options[:perpage]
                    index = options[:page]*options[:perpage]
                    data_set=data_set[index..(index+options[:perpage]-1)]
                end
                data_set.map(&:load_content).map{|x| x[:data]}
            end

            def load_execution_plan(execution_plan_id)
                DynflowExecutionPlanStorage.find_by(:uuid=>execution_plan_id).load_content[:data]
            end

            def save_execution_plan(execution_plan_id, value)
                plan = DynflowExecutionPlanStorage.find_or_create_by :uuid=>execution_plan_id
                plan.save_content! value
            end

            def load_step(execution_plan_id, step_id)
                plan=DynflowExecutionPlanStorage.find_by :uuid=>execution_plan_id
                plan.dynflow_step_storages.where(:step_id=>step_id).first.load_content[:data]
            end

            def save_step(execution_plan_id, step_id, value)
                plan=DynflowExecutionPlanStorage.find_by :uuid=>execution_plan_id
                step=plan.dynflow_step_storages.find_or_create_by :step_id=>step_id
                step.save_content! value
            end

            def load_action(execution_plan_id, action_id)
                plan=DynflowExecutionPlanStorage.find_by :uuid=>execution_plan_id
                plan.dynflow_action_storages.where(:action_id=>action_id).first.load_content[:data]
            end

            def save_action(execution_plan_id, action_id, value)
                plan=DynflowExecutionPlanStorage.find_by :uuid=>execution_plan_id
                action=plan.dynflow_action_storages.find_or_create_by :action_id=>action_id
                action.save_content! value
            end

            def to_hash
                { execution_plans: DynflowExecutionPlanStorage.all.map(&:load_content),
                    steps:           DynflowStepStorage.all.map(&:load_content),
                    actions:         DynflownActionStorage.all.map(&:load_content) }
            end
        end
    end
end
