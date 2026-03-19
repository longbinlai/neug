/**
 * Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <gtest/gtest.h>
#include "lif_sim_engine.h"

using namespace lif_sim;

class LIFSimStateTest : public ::testing::Test {
protected:
    LIFSimState state;

    void SetUp() override {
        state.clear();
    }
};

TEST_F(LIFSimStateTest, InitNeuronTest) {
    state.initNeuron(100);
    EXPECT_TRUE(state.hasNeuron(100));
    EXPECT_FALSE(state.hasNeuron(200));
    EXPECT_EQ(state.getNeuronCount(), 1);

    state.initNeuron(200);
    state.initNeuron(300);
    EXPECT_EQ(state.getNeuronCount(), 3);
}

TEST_F(LIFSimStateTest, NeuronStateDefaultsTest) {
    state.initNeuron(1);
    const auto& params = LIFSimState::getParams();
    auto& ns = state.getState(1);

    EXPECT_DOUBLE_EQ(ns.membrane_potential, params.v_rest);
    EXPECT_FALSE(ns.spiked);
    EXPECT_EQ(ns.spike_count, 0);
    EXPECT_EQ(ns.spike_round, -1);
}

TEST_F(LIFSimStateTest, SetStimulusTest) {
    state.initNeuron(1);
    state.initNeuron(2);

    state.setStimulus(1, 60.0);
    state.setStimulus(2, 30.0);

    EXPECT_DOUBLE_EQ(state.getStimulusAmplitude(1), 60.0);
    EXPECT_DOUBLE_EQ(state.getStimulusAmplitude(2), 30.0);
    EXPECT_DOUBLE_EQ(state.getStimulusAmplitude(999), 0.0);  // not set

    const auto& all = state.getAllStimuli();
    EXPECT_EQ(all.size(), 2u);
}

TEST_F(LIFSimStateTest, StimulusOverwriteTest) {
    state.initNeuron(1);
    state.setStimulus(1, 60.0);
    state.setStimulus(1, 80.0);  // overwrite
    EXPECT_DOUBLE_EQ(state.getStimulusAmplitude(1), 80.0);
}

TEST_F(LIFSimStateTest, ClearTest) {
    state.initNeuron(1);
    state.initNeuron(2);
    state.setStimulus(1, 60.0);

    EXPECT_EQ(state.getNeuronCount(), 2);

    state.clear();
    EXPECT_EQ(state.getNeuronCount(), 0);
    EXPECT_FALSE(state.hasNeuron(1));
    EXPECT_EQ(state.getAllStimuli().size(), 0u);
}

TEST_F(LIFSimStateTest, SynapseSignTest) {
    // Drosophila convention: ACH excitatory, GABA/GLUT inhibitory
    EXPECT_DOUBLE_EQ(LIFSimState::getSynapseSign("ACH"), 1.0);
    EXPECT_DOUBLE_EQ(LIFSimState::getSynapseSign("GABA"), -1.0);
    EXPECT_DOUBLE_EQ(LIFSimState::getSynapseSign("GLUT"), -1.0);
    EXPECT_DOUBLE_EQ(LIFSimState::getSynapseSign("DA"), 0.0);
    EXPECT_DOUBLE_EQ(LIFSimState::getSynapseSign("SER"), 0.0);
    EXPECT_DOUBLE_EQ(LIFSimState::getSynapseSign("UNKNOWN"), 0.0);
}

TEST_F(LIFSimStateTest, SpikeRoundTrackingTest) {
    // Simulate manual spike tracking (as wave propagation would do)
    state.initNeuron(1);
    state.initNeuron(2);
    const auto& params = LIFSimState::getParams();

    // Neuron 1: stimulus pushes above threshold
    auto& s1 = state.getState(1);
    s1.membrane_potential += 25.0;  // -70 + 25 = -45, above threshold -50? No.
    // -70 + 25 = -45 > -50, so it spikes
    EXPECT_GE(s1.membrane_potential, params.v_threshold);
    s1.spiked = true;
    s1.spike_count = 1;
    s1.spike_round = 0;

    EXPECT_EQ(s1.spike_round, 0);
    EXPECT_EQ(s1.spike_count, 1);

    // Neuron 2: later round
    auto& s2 = state.getState(2);
    s2.membrane_potential += 25.0;
    s2.spiked = true;
    s2.spike_count = 1;
    s2.spike_round = 1;

    EXPECT_EQ(s2.spike_round, 1);
    EXPECT_EQ(state.getTotalSpikeCount(), 2);
}

TEST_F(LIFSimStateTest, ParamsTest) {
    const auto& params = LIFSimState::getParams();
    EXPECT_DOUBLE_EQ(params.v_rest, -70.0);
    EXPECT_DOUBLE_EQ(params.v_threshold, -50.0);
    EXPECT_DOUBLE_EQ(params.v_reset, -65.0);
    EXPECT_DOUBLE_EQ(params.w_syn, 0.09);
    EXPECT_DOUBLE_EQ(params.tau_m, 20.0);
}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
