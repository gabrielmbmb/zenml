#  Copyright (c) ZenML GmbH 2022. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at:
#
#       https://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
#  or implied. See the License for the specific language governing
#  permissions and limitations under the License.

from zenml.pipelines import pipeline


@pipeline(enable_cache=False)
def slack_ask_pipeline(
    data_loader,
    trainer,
    evaluator,
    formatter,
    alerter,
    deployer
):
    """Train and evaluate a model and asks user in Slack whether to deploy."""
    X_train, X_test, y_train, y_test = data_loader()
    model = trainer(X_train=X_train, y_train=y_train)
    test_acc = evaluator(X_test=X_test, y_test=y_test, model=model)
    message = formatter(test_acc)
    approved = alerter(message)
    deployer(approved, model)
